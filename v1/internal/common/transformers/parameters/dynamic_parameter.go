package parameters

import (
	"bytes"
	"fmt"
	"slices"
	"text/template"

	"github.com/rs/zerolog/log"

	commoninterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

var (
	errCheckTransformerImplementation        = fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	errReceivedNullValueFromDynamicParameter = fmt.Errorf("received NULL value from dynamic parameter and default value is not set")
)

type DynamicParameter struct {
	// DynamicValue - The dynamic value settings that received from config.
	DynamicValue *commonmodels.DynamicParamValue
	// definition - the parameter definition
	definition *ParameterDefinition
	// tableDriver - table driver.
	tableDriver commoninterfaces.TableDriver
	// record - Record object for getting the value from record dynamically
	record commoninterfaces.Recorder
	// tmpl - parsed and compiled template for casting the value from original to expected
	tmpl *template.Template
	// linkedColumnParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	linkedColumnParameter *StaticParameter
	// columnIdx - column number in the tuple
	columnIdx int
	buf       *bytes.Buffer
	column    *commonmodels.Column
	//defaultValueFromDynamicParamValue any
	//defaultValueFromDefinition        any

	hasDefaultValue     bool
	defaultValueScanned any
	defaultValueGot     any
	rawDefaultValue     commonmodels.ParamsValue
	tmplCtx             *DynamicParameterContext
	castToFunc          gmtemplate.TypeCastFunc
	// columnCanonicalTypeName - canonical type name of the column that is used for dynamic parameter.
	columnCanonicalTypeName string
}

func NewDynamicParameter(def *ParameterDefinition, driver commoninterfaces.TableDriver) *DynamicParameter {
	return &DynamicParameter{
		definition:  def,
		tableDriver: driver,
		buf:         bytes.NewBuffer(nil),
	}
}

func (dp *DynamicParameter) IsEmpty() (bool, error) {
	if dp.record == nil {
		return false, fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	}

	rawValue, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return false, fmt.Errorf("erro getting raw column value: %w", err)
	}

	if !rawValue.IsNull {
		return false, nil
	}

	return !dp.hasDefaultValue, nil
}

func (dp *DynamicParameter) Name() string {
	return dp.definition.Name
}

func (dp *DynamicParameter) IsDynamic() bool {
	return true
}

func (dp *DynamicParameter) GetDefinition() *ParameterDefinition {
	return dp.definition
}

func (dp *DynamicParameter) SetRecord(r commoninterfaces.Recorder) {
	dp.record = r
	dp.tmplCtx.setRecord(r)
}

func (dp *DynamicParameter) Init(
	vc *validationcollector.Collector,
	columnParameters map[string]*StaticParameter,
	dynamicValue commonmodels.DynamicParamValue,
) error {
	dp.DynamicValue = &dynamicValue

	// Validate dynamic value
	dp.validate(vc, dp.DynamicValue)
	if vc.IsFatal() {
		return commonmodels.ErrFatalValidationError
	}

	// Determine default value
	dp.determineDefaultValue()

	// Render template
	dp.renderTemplate(vc)
	if vc.IsFatal() {
		return commonmodels.ErrFatalValidationError
	}

	// Initialize dynamic parameter context
	// It gets column by name and sets it to the context
	dp.initDynamicParameterContext(vc)
	if vc.IsFatal() {
		return commonmodels.ErrFatalValidationError
	}

	// Initialize cast_to function
	dp.initCastTo(vc)
	if vc.IsFatal() {
		return commonmodels.ErrFatalValidationError
	}

	// Initialize link column parameter
	err := dp.initLinkColumnParameter(vc, columnParameters)
	if err != nil {
		return fmt.Errorf("initialize link column parameter: %w", err)
	}
	if vc.IsFatal() {
		return commonmodels.ErrFatalValidationError
	}
	return nil
}

func (dp *DynamicParameter) validate(
	vc *validationcollector.Collector,
	v *commonmodels.DynamicParamValue,
) {
	if dp.definition.DynamicModeProperties == nil {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg("parameter does not support dynamic mode"))
		return
	}

	if v == nil {
		panic("dynamicValue is nil: possibly bug")
	}

	if dp.DynamicValue.Column == "" {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg("received empty \"column\" parameter").
			AddMeta("DynamicParameterSetting", "column"))
	}

	if dp.definition.IsColumn {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg("column parameter cannot work in dynamic mode"))
	}
}

func (dp *DynamicParameter) determineDefaultValue() {
	if dp.DynamicValue.DefaultValue != nil {
		dp.rawDefaultValue = slices.Clone(dp.DynamicValue.DefaultValue)
		dp.hasDefaultValue = true
	} else if dp.definition.DefaultValue != nil {
		dp.rawDefaultValue = slices.Clone(dp.definition.DefaultValue)
		dp.hasDefaultValue = true
	}
}

func (dp *DynamicParameter) renderTemplate(vc *validationcollector.Collector) {
	if dp.DynamicValue.Template == "" {
		return
	}
	var err error
	dp.tmpl, err = template.New("").
		Funcs(gmtemplate.FuncMap()).
		Parse(dp.DynamicValue.Template)
	if err != nil {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg("unable to render cast template").
			AddMeta("Error", err.Error()).
			AddMeta("DynamicParameterSetting", "cast_template"))
	}
}

func (dp *DynamicParameter) initDynamicParameterContext(vc *validationcollector.Collector) {
	column, ok := dp.tableDriver.GetColumnByName(dp.DynamicValue.Column)
	if !ok {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg("column does not exist").
			AddMeta("DynamicParameterSetting", "column").
			AddMeta("ColumnName", dp.definition.Name))
		return
	}
	dp.column = column
	dp.columnIdx = column.Idx
	dp.tmplCtx = NewDynamicParameterContext(column)
	return
}

func (dp *DynamicParameter) initCastTo(vc *validationcollector.Collector) {
	if dp.DynamicValue.CastTo == "" {
		return
	}
	// TODO: Implement cast_to function execution for any type of DBMS.
	panic("IMPLEMENT ME")

	//var castFuncDef *gmtemplate.TypeCastDefinition
	//if dp.definition.LinkColumnParameter == "" {
	//	vc.Add(commonmodels.NewValidationWarning().
	//		SetSeverity(commonmodels.ValidationSeverityError).
	//		SetMsg("cast_to parameter is not supported for Non Linked transformer parameters").
	//		AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
	//		AddMeta("DynamicParameterSetting", "cast_to"))
	//	return
	//}
	//
	//castFuncDef, ok := gmtemplate.CastFunctionsMap[dp.DynamicValue.CastTo]
	//if !ok {
	//	vc.Add(commonmodels.NewValidationWarning().
	//		SetSeverity(commonmodels.ValidationSeverityError).
	//		SetMsg("unable to find cast_to function").
	//		AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
	//		AddMeta("DynamicParameterSetting", "cast_to"))
	//	return
	//}
	//dp.castToFunc = castFuncDef.Cast
}

func (dp *DynamicParameter) initLinkColumnParameter(
	vc *validationcollector.Collector,
	columnParameters map[string]*StaticParameter,
) (err error) {
	if dp.definition.LinkColumnParameter == "" {
		return nil
	}
	param, ok := columnParameters[dp.definition.LinkColumnParameter]
	if !ok {
		panic(fmt.Sprintf(`parameter with name "%s" is not found`, dp.definition.LinkColumnParameter))
	}
	dp.linkedColumnParameter = param
	if !dp.linkedColumnParameter.definition.IsColumn {
		panic("bug: linked parameter must be column: check transformer implementation")
	}
	dp.tmplCtx.setLinkedColumn(dp.linkedColumnParameter.Column)

	dp.columnCanonicalTypeName, err = dp.tableDriver.GetCanonicalTypeName(dp.column.TypeName, dp.column.TypeOID)
	if err != nil {
		return fmt.Errorf("get input canonical type name: %w", err)
	}
	linkedParameterColumnType, err := dp.tableDriver.GetCanonicalTypeName(
		dp.linkedColumnParameter.Column.TypeName,
		dp.linkedColumnParameter.Column.TypeOID,
	)
	if err != nil {
		return fmt.Errorf("get output canonical type name: %w", err)
	}

	if dp.tmpl == nil && dp.castToFunc == nil {
		// Check that column parameter has the same type with dynamic parameter value or at least dynamic parameter
		// column is compatible with type in the list. This logic is controversial since it might be unexpected
		// when dynamic param column has different though compatible types. Consider it
		if linkedParameterColumnType != dp.columnCanonicalTypeName &&
			dp.linkedColumnParameter.definition.ColumnProperties != nil &&
			len(dp.linkedColumnParameter.definition.ColumnProperties.AllowedTypes) > 0 &&
			!slices.Contains(dp.linkedColumnParameter.definition.ColumnProperties.AllowedTypes, dp.columnCanonicalTypeName) {
			vc.Add(commonmodels.NewValidationWarning().
				SetSeverity(commonmodels.ValidationSeverityError).
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("DynamicParameterColumnType", dp.column.TypeName).
				AddMeta("DynamicParameterColumnName", dp.column.Name).
				AddMeta("LinkedParameterName", dp.definition.LinkColumnParameter).
				AddMeta("LinkedColumnName", dp.linkedColumnParameter.Column.Name).
				AddMeta("LinkedColumnType", dp.linkedColumnParameter.Column.TypeName).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				SetMsg("linked parameter and dynamic parameter column name has different types"))
		}
	}
	return nil
}

func (dp *DynamicParameter) Value() (value any, err error) {
	dp.buf.Reset()
	if dp.record == nil {
		return nil, errCheckTransformerImplementation
	}

	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("erro getting raw column value: %w", err)
	}

	if v.IsNull {
		if !dp.hasDefaultValue {
			return nil, fmt.Errorf("received NULL value from dynamic parameter")
		}
		if dp.defaultValueGot == nil {
			res, err := getValue(dp.tableDriver, dp.definition, dp.rawDefaultValue, dp.linkedColumnParameter)
			if err != nil {
				return nil, err
			}
			dp.defaultValueGot = res
		}
		return dp.defaultValueGot, nil
	}

	rawValue := v.Data

	if dp.tmpl != nil {
		// If a template is defined, we execute it to cast the value
		// and then use the result as a raw value.
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.tableDriver.Table().Schema).
				Str("FullTableName", dp.tableDriver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return nil, fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil {
		panic("IMPLEMENT ME")
		// TODO: Implement cast_to function execution for any type of DBMS.
		//rawValue, err = dp.castToFunc(dp.tableDriver, rawValue)
		//if err != nil {
		//	log.Debug().
		//		Err(err).
		//		Str("ParameterName", dp.definition.Name).
		//		Str("RawValue", string(v.Data)).
		//		Str("TableSchema", dp.tableDriver.Table().Schema).
		//		Str("FullTableName", dp.tableDriver.Table().Name).
		//		Str("Error", err.Error()).
		//		Msg("error executing cast_to function")
		//
		//	return nil, fmt.Errorf("error executing cast_to function: %w", err)
		//}
	}

	// Now get the raw value and decode it using the tableDriver or custom unmarshaller if defined.
	if dp.definition.DynamicModeProperties.Unmarshal != nil {
		res, err := dp.definition.DynamicModeProperties.Unmarshal(dp.tableDriver, dp.columnCanonicalTypeName, rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return res, nil
	}

	// Decode the value using the tableDriver - it's default behaviour.
	res, err := dp.tableDriver.DecodeValueByColumnIdx(dp.columnIdx, rawValue)
	if err != nil {
		log.Debug().
			Err(err).
			Str("ParameterName", dp.definition.Name).
			Str("RawValue", string(v.Data)).
			Str("CastedValue", string(rawValue)).
			Str("TableSchema", dp.tableDriver.Table().Schema).
			Str("FullTableName", dp.tableDriver.Table().Name).
			Msg("error decoding casted value")

		return nil, fmt.Errorf("error scanning casted value: %w", err)
	}
	return res, nil

}

func (dp *DynamicParameter) RawValue() (commonmodels.ParamsValue, error) {
	if dp.record == nil {
		return nil, errCheckTransformerImplementation
	}

	rawValue, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, err
	}
	if rawValue.IsNull {
		if dp.hasDefaultValue {
			return dp.rawDefaultValue, nil
		}
		return nil, fmt.Errorf("received NULL value from dynamic parameter")
	}
	return rawValue.Data, nil
}

func (dp *DynamicParameter) Scan(dest any) error {
	dp.buf.Reset()
	if dp.record == nil {
		return errCheckTransformerImplementation
	}

	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return fmt.Errorf("erro getting raw column value: %w", err)
	}

	if v.IsNull {
		if !dp.hasDefaultValue {
			return errReceivedNullValueFromDynamicParameter
		}

		if dp.defaultValueScanned == nil {
			err = scanValue(dp.tableDriver, dp.definition, dp.rawDefaultValue, dp.linkedColumnParameter, dest)
			if err != nil {
				return err
			}
			// TODO: You must copy scanned value since the dest is the pointer receiver otherwise it will cause
			// 	unexpected behaviour
			dp.defaultValueScanned = dest
			return nil
		}
		return utils.ScanPointer(dp.defaultValueScanned, dest)
	}

	rawValue := v.Data

	if dp.tmpl != nil {
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.tableDriver.Table().Schema).
				Str("FullTableName", dp.tableDriver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil {
		panic("IMPLEMENT ME")
		// TODO: Implement cast_to function execution for any type of DBMS.
		//rawValue, err = dp.castToFunc(dp.tableDriver, rawValue)
		//if err != nil {
		//	log.Debug().
		//		Err(err).
		//		Str("ParameterName", dp.definition.Name).
		//		Str("RawValue", string(v.Data)).
		//		Str("TableSchema", dp.tableDriver.Table().Schema).
		//		Str("FullTableName", dp.tableDriver.Table().Name).
		//		Str("Error", err.Error()).
		//		Msg("error executing cast_to function")
		//
		//	return fmt.Errorf("error executing cast_to function: %w", err)
		//}
	}

	if dp.definition.DynamicModeProperties.Unmarshal != nil {
		value, err := dp.definition.DynamicModeProperties.Unmarshal(dp.tableDriver, dp.columnCanonicalTypeName, rawValue)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return utils.ScanPointer(value, dest)
	}

	err = scanValue(dp.tableDriver, dp.definition, rawValue, dp.linkedColumnParameter, dest)
	if err != nil {
		log.Debug().
			Err(err).
			Str("ParameterName", dp.definition.Name).
			Str("RawValue", string(v.Data)).
			Str("CastedValue", string(rawValue)).
			Str("TableSchema", dp.tableDriver.Table().Schema).
			Str("FullTableName", dp.tableDriver.Table().Name).
			Msg("error decoding casted value")

		return fmt.Errorf("error scanning casted value: %w", err)
	}
	return nil
}
