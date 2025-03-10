package parameters

import (
	"bytes"
	"fmt"
	"slices"
	"text/template"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	gmtemplate "github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
	"github.com/greenmaskio/greenmask/v1/internal/utils"
)

type driver interface {
	DecodeValueByColumnIdx(idx int, src []byte) (any, error)
	EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error)
	GetColumnByName(name string) (*models.Column, bool)
	DecodeValueByTypeName(name string, src []byte) (any, error)
	GetCanonicalTypeName(name string, oid models.VirtualOID) (string, error)
	DecodeValueByTypeOid(oid uint32, src []byte) (any, error)
	EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error)
	Table() *models.Table
	GetTypeOIDByName(typeName string) (uint32, bool)
}

type DynamicParameter struct {
	// DynamicValue - The dynamic value settings that received from config
	DynamicValue *models.DynamicParamValue
	// definition - the parameter definition
	definition *ParameterDefinition
	// Driver - table driver
	driver driver
	// record - Record object for getting the value from record dynamically
	record record
	// tmpl - parsed and compiled template for casting the value from original to expected
	tmpl *template.Template
	// linkedColumnParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	linkedColumnParameter *StaticParameter
	// columnIdx - column number in the tuple
	columnIdx int
	buf       *bytes.Buffer
	column    *models.Column
	//defaultValueFromDynamicParamValue any
	//defaultValueFromDefinition        any

	hasDefaultValue     bool
	defaultValueScanned any
	defaultValueGot     any
	rawDefaultValue     models.ParamsValue
	tmplCtx             *DynamicParameterContext
	castToFunc          gmtemplate.TypeCastFunc
}

func NewDynamicParameter(def *ParameterDefinition, driver driver) *DynamicParameter {
	return &DynamicParameter{
		definition: def,
		driver:     driver,
		buf:        bytes.NewBuffer(nil),
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

func (dp *DynamicParameter) IsDynamic() bool {
	return true
}

func (dp *DynamicParameter) GetDefinition() *ParameterDefinition {
	return dp.definition
}

func (dp *DynamicParameter) SetRecord(r record) {
	dp.record = r
	dp.tmplCtx.setRecord(r)
}

func (dp *DynamicParameter) Init(
	columnParameters map[string]*StaticParameter,
	dynamicValue models.DynamicParamValue,
) (models.ValidationWarnings, error) {
	dp.DynamicValue = &dynamicValue
	var warnings models.ValidationWarnings

	// Validate dynamic value
	warnings = append(warnings, dp.validate(dp.DynamicValue)...)
	if warnings.IsFatal() {
		return warnings, nil
	}

	// Determine default value
	dp.determineDefaultValue()

	// Render template
	warnings = append(warnings, dp.renderTemplate()...)
	if warnings.IsFatal() {
		return warnings, nil
	}

	// Initialize dynamic parameter context
	// It gets column by name and sets it to the context
	warnings = append(warnings, dp.initDynamicParameterContext()...)
	if warnings.IsFatal() {
		return warnings, nil
	}

	// Initialize cast_to function
	warnings = append(warnings, dp.initCastTo()...)
	if warnings.IsFatal() {
		return warnings, nil
	}

	// Initialize link column parameter
	initLinkColParamWarnings, err := dp.initLinkColumnParameter(columnParameters)
	if err != nil {
		return warnings, err
	}
	warnings = append(warnings, initLinkColParamWarnings...)

	return warnings, nil
}

func (dp *DynamicParameter) validate(v *models.DynamicParamValue) models.ValidationWarnings {
	var warnings models.ValidationWarnings
	if dp.definition.DynamicModeProperties == nil {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("parameter does not support dynamic mode"),
		)
		return warnings
	}

	if v == nil {
		panic("dynamicValue is nil: possibly bug")
	}

	if dp.DynamicValue.Column == "" {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("received empty \"column\" parameter").
				AddMeta("DynamicParameterSetting", "column"),
		)
	}

	if dp.definition.IsColumn {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("column parameter cannot work in dynamic mode"),
		)
	}

	return warnings
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

func (dp *DynamicParameter) renderTemplate() models.ValidationWarnings {
	if dp.DynamicValue.Template == "" {
		return nil
	}
	var warnings models.ValidationWarnings
	var err error
	dp.tmpl, err = template.New("").
		Funcs(gmtemplate.FuncMap()).
		Parse(dp.DynamicValue.Template)
	if err != nil {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("unable to render cast template").
				AddMeta("Error", err.Error()).
				AddMeta("DynamicParameterSetting", "cast_template"),
		)
		return warnings
	}
	return nil
}

func (dp *DynamicParameter) initDynamicParameterContext() models.ValidationWarnings {
	column, ok := dp.driver.GetColumnByName(dp.DynamicValue.Column)
	if !ok {
		return models.ValidationWarnings{
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("column does not exist").
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("ColumnName", dp.definition.Name),
		}
	}
	dp.column = column
	dp.columnIdx = column.Idx
	dp.tmplCtx = NewDynamicParameterContext(column)
	return nil
}

func (dp *DynamicParameter) initCastTo() models.ValidationWarnings {
	if dp.DynamicValue.CastTo == "" {
		return nil
	}

	var castFuncDef *gmtemplate.TypeCastDefinition
	var warnings models.ValidationWarnings
	if dp.definition.LinkColumnParameter == "" {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("cast_to parameter is not supported for Non Linked transformer parameters").
				AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
				AddMeta("DynamicParameterSetting", "cast_to"),
		)
		return warnings
	}

	castFuncDef, ok := gmtemplate.CastFunctionsMap[dp.DynamicValue.CastTo]
	if !ok {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("unable to find cast_to function").
				AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
				AddMeta("DynamicParameterSetting", "cast_to"),
		)
		return warnings
	}
	dp.castToFunc = castFuncDef.Cast
	return nil
}

func (dp *DynamicParameter) initLinkColumnParameter(
	columnParameters map[string]*StaticParameter,
) (models.ValidationWarnings, error) {
	if dp.definition.LinkColumnParameter == "" {
		return nil, nil
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

	inputType, err := dp.driver.GetCanonicalTypeName(dp.column.Type, dp.column.TypeOID)
	if err != nil {
		return nil, fmt.Errorf("get input canonical type name: %w", err)
	}
	//outputType := GetCanonicalTypeName(dp.driver, dp.linkedColumnParameter.Column.TypeName, uint32(dp.linkedColumnParameter.Column.TypeOid))
	outputType, err := dp.driver.GetCanonicalTypeName(
		dp.linkedColumnParameter.Column.Type,
		dp.linkedColumnParameter.Column.TypeOID,
	)
	if err != nil {
		return nil, fmt.Errorf("get output canonical type name: %w", err)
	}

	var warnings models.ValidationWarnings
	if castFuncDef != nil && !castFuncDef.ValidateTypes(inputType, outputType) {
		warnings = append(
			warnings,
			models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("type cast function has unsupported input or output types").
				AddMeta("AllowedInputTypes", castFuncDef.InputTypes).
				AddMeta("AllowedOutputTypes", castFuncDef.OutputTypes).
				AddMeta("RequestedInputType", inputType).
				AddMeta("RequestedOutputType", outputType).
				AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
				AddMeta("DynamicParameterSetting", "cast_to"),
		)
		return warnings, nil
	}

	// TODO: There is bug with column overriding type since OverriddenTypeOid is not checking
	// TODO: Add SupportedTypes checking there. Consider IsTypeAllowedWithTypeMap usage
	if dp.tmpl == nil && dp.castToFunc == nil {
		// Check that column parameter has the same type with dynamic parameter value or at least dynamic parameter
		// column is compatible with type in the list. This logic is controversial since it might be unexpected
		// when dynamic param column has different though compatible types. Consider it
		if dp.linkedColumnParameter.Column.TypeOID != dp.column.TypeOID &&
			dp.linkedColumnParameter.definition.ColumnProperties != nil &&
			len(dp.linkedColumnParameter.definition.ColumnProperties.AllowedTypes) > 0 &&
			!IsTypeAllowedWithTypeMap(
				dp.driver,
				dp.definition.DynamicModeProperties.SupportedTypes,
				dp.column.Type,
				dp.column.TypeOID,
				true,
			) {
			warnings = append(warnings, models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("DynamicParameterColumnType", dp.column.Type).
				AddMeta("DynamicParameterColumnName", dp.column.Name).
				AddMeta("LinkedParameterName", dp.definition.LinkColumnParameter).
				AddMeta("LinkedColumnName", dp.linkedColumnParameter.Column.Name).
				AddMeta("LinkedColumnType", dp.linkedColumnParameter.Column.Type).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				SetMsg("linked parameter and dynamic parameter column name has different types"),
			)
		}

		if dp.definition.CastDbType != "" &&
			!IsTypeAllowedWithTypeMap(
				dp.driver,
				[]string{dp.definition.CastDbType},
				dp.column.Type,
				dp.column.TypeOID,
				true,
			) {
			warnings = append(warnings, models.NewValidationWarning().
				SetSeverity(models.ErrorValidationSeverity).
				SetMsg("unsupported column type: unsupported type according cast_db_type").
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("DynamicParameterColumnType", dp.column.Type).
				AddMeta("DynamicParameterColumnName", dp.column.Name).
				AddMeta("CastDbType", dp.definition.CastDbType),
			)
		}
	}

}

func (dp *DynamicParameter) Value() (value any, err error) {
	dp.buf.Reset()
	if dp.record == nil {
		return nil, fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
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
			res, err := getValue(dp.driver, dp.definition, dp.rawDefaultValue, dp.linkedColumnParameter)
			if err != nil {
				return nil, err
			}
			dp.defaultValueGot = res
		}
		return dp.defaultValueGot, nil
	}

	rawValue := v.Data

	if dp.tmpl != nil {
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table().Schema).
				Str("TableName", dp.driver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return nil, fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil {
		rawValue, err = dp.castToFunc(dp.driver, rawValue)
		if err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table().Schema).
				Str("TableName", dp.driver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast_to function")

			return nil, fmt.Errorf("error executing cast_to function: %w", err)
		}
	}

	if dp.definition.DynamicModeProperties.Unmarshal != nil {
		res, err := dp.definition.DynamicModeProperties.Unmarshal(dp.driver, dp.column.CanonicalTypeName, rawValue)
		if err != nil {
			return nil, fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return res, nil
	}

	res, err := dp.driver.DecodeValueByColumnIdx(dp.columnIdx, rawValue)
	if err != nil {
		log.Debug().
			Err(err).
			Str("ParameterName", dp.definition.Name).
			Str("RawValue", string(v.Data)).
			Str("CastedValue", string(rawValue)).
			Str("TableSchema", dp.driver.Table().Schema).
			Str("TableName", dp.driver.Table().Name).
			Msg("error decoding casted value")

		return nil, fmt.Errorf("error scanning casted value: %w", err)
	}
	return res, nil

}

func (dp *DynamicParameter) RawValue() (models.ParamsValue, error) {
	if dp.record == nil {
		return nil, fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
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
		return fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	}

	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return fmt.Errorf("erro getting raw column value: %w", err)
	}

	if v.IsNull {
		if !dp.hasDefaultValue {
			return fmt.Errorf("received NULL value from dynamic parameter")
		}

		if dp.defaultValueScanned == nil {
			err = scanValue(dp.driver, dp.definition, dp.rawDefaultValue, dp.linkedColumnParameter, dest)
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

	var usedDefaultValue bool
	if dp.tmpl != nil && !usedDefaultValue {
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table().Schema).
				Str("TableName", dp.driver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil && !usedDefaultValue {
		rawValue, err = dp.castToFunc(dp.driver, rawValue)
		if err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table().Schema).
				Str("TableName", dp.driver.Table().Name).
				Str("Error", err.Error()).
				Msg("error executing cast_to function")

			return fmt.Errorf("error executing cast_to function: %w", err)
		}
	}

	if dp.definition.DynamicModeProperties.Unmarshal != nil {
		value, err := dp.definition.DynamicModeProperties.Unmarshal(dp.driver, dp.column.CanonicalTypeName, rawValue)
		if err != nil {
			return fmt.Errorf("unable to perform custom unmarshaller: %w", err)
		}
		return utils.ScanPointer(value, dest)
	}

	err = scanValue(dp.driver, dp.definition, rawValue, dp.linkedColumnParameter, dest)
	if err != nil {
		return err
	}

	//err = dp.driver.ScanValueByColumnIdx(dp.columnIdx, rawValue, dest)
	if err != nil {
		log.Debug().
			Err(err).
			Str("ParameterName", dp.definition.Name).
			Str("RawValue", string(v.Data)).
			Str("CastedValue", string(rawValue)).
			Str("TableSchema", dp.driver.Table().Schema).
			Str("TableName", dp.driver.Table().Name).
			Msg("error decoding casted value")

		return fmt.Errorf("error scanning casted value: %w", err)
	}
	return nil
}
