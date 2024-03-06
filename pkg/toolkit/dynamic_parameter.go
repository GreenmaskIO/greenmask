package toolkit

import (
	"bytes"
	"fmt"
	"slices"
	"text/template"

	"github.com/rs/zerolog/log"
)

// TODO:
//		Add tests for:
// 			1. Custom Unmarshaller function execution for Value and Scan
//  		2. Test cast template and cast functions for it
//			3. Test defaultValue caching after decoding - defaultValueScanned and defaultValueGot
//			4. Test default values behaviour when dynamic value IsNull
//		Implement:
//			1. Smart scanning - it must be possible scan compatible types values like int32 into int64. Add feature that
//			   allows to scan not pointer value into pointer receiver

type DynamicParameterContext struct {
	//columnType string
	column       *Column
	linkedColumn *Column
	rc           *RecordContext
}

func NewDynamicParameterContext(column *Column) *DynamicParameterContext {
	if column == nil {
		panic("column cannot be nil")
	}

	return &DynamicParameterContext{
		column: column,
		rc:     &RecordContext{},
	}
}

func (dpc *DynamicParameterContext) setLinkedColumn(linkedColumn *Column) {
	dpc.linkedColumn = linkedColumn
}

func (dpc *DynamicParameterContext) clean() {
	dpc.rc.Clean()
}

func (dpc *DynamicParameterContext) setRecord(r *Record) {
	dpc.rc.SetRecord(r)
}

func (dpc *DynamicParameterContext) GetColumnType() string {
	return dpc.column.TypeName
}

func (dpc *DynamicParameterContext) GetValue() (any, error) {
	return dpc.rc.GetColumnValue(dpc.column.Name)
}

func (dpc *DynamicParameterContext) GetRawValue() (any, error) {
	return dpc.rc.GetRawColumnValue(dpc.column.Name)
}

func (dpc *DynamicParameterContext) GetColumnValue(name string) (any, error) {
	return dpc.rc.GetColumnValue(name)
}

func (dpc *DynamicParameterContext) GetColumnRawValue(name string) (any, error) {
	return dpc.rc.GetRawColumnValue(name)
}

func (dpc *DynamicParameterContext) EncodeValue(v any) (any, error) {
	if dpc.linkedColumn == nil {
		return nil, fmt.Errorf("unable to encode not linked prameter use .EncodeValueByColumn or EncodeValueByType intead")
	}
	return dpc.rc.EncodeValueByColumn(dpc.linkedColumn.Name, v)
}

func (dpc *DynamicParameterContext) DecodeValue(v any) (any, error) {
	if dpc.linkedColumn == nil {
		return nil, fmt.Errorf("unable to decode not linked prameter use .DecodeValueByColumn or DecodeValueByType intead")
	}
	return dpc.rc.DecodeValueByColumn(dpc.linkedColumn.TypeName, v)
}

func (dpc *DynamicParameterContext) EncodeValueByColumn(name string, v any) (any, error) {
	return dpc.rc.EncodeValueByColumn(name, v)
}

func (dpc *DynamicParameterContext) DecodeValueByColumn(name string, v any) (any, error) {
	return dpc.rc.DecodeValueByColumn(name, v)
}

func (dpc *DynamicParameterContext) EncodeValueByType(name string, v any) (any, error) {
	return dpc.rc.EncodeValueByType(name, v)
}

func (dpc *DynamicParameterContext) DecodeValueByType(name string, v any) (any, error) {
	return dpc.rc.DecodeValueByType(name, v)
}

type DynamicParameter struct {
	// DynamicValue - The dynamic value settings that received from config
	DynamicValue *DynamicParamValue
	// definition - the parameter definition
	definition *ParameterDefinition
	// Driver - table driver
	driver *Driver
	// record - Record object for getting the value from record dynamically
	record *Record
	// tmpl - parsed and compiled template for casting the value from original to expected
	tmpl *template.Template
	// linkedColumnParameter - column-like parameter that has been linked during parsing procedure. Warning, do not
	// assign it manually, if you don't know the consequences
	linkedColumnParameter *StaticParameter
	// columnIdx - column number in the tuple
	columnIdx int
	buf       *bytes.Buffer
	column    *Column
	//defaultValueFromDynamicParamValue any
	//defaultValueFromDefinition        any

	hasDefaultValue     bool
	defaultValueScanned any
	defaultValueGot     any
	rawDefaultValue     ParamsValue
	tmplCtx             *DynamicParameterContext
	castToFunc          TypeCastFunc
}

func NewDynamicParameter(def *ParameterDefinition, driver *Driver) *DynamicParameter {
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

func (dp *DynamicParameter) SetRecord(r *Record) {
	dp.record = r
	dp.tmplCtx.setRecord(r)
}

func (dp *DynamicParameter) Init(columnParameters map[string]*StaticParameter, dynamicValue *DynamicParamValue) (warnings ValidationWarnings, err error) {

	if dp.definition.DynamicModeProperties == nil {
		warnings = append(
			warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				SetMsg("parameter does not support dynamic mode"),
		)
		return warnings, nil
	}

	if dynamicValue == nil {
		panic("DynamicValue is nil")
	}
	dp.DynamicValue = dynamicValue

	if dp.DynamicValue.Column == "" {
		warnings = append(
			warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				SetMsg("received empty \"column\" parameter").
				AddMeta("DynamicParameterSetting", "column"),
		)
		return warnings, nil
	}

	if dp.DynamicValue.DefaultValue != nil {
		dp.rawDefaultValue = slices.Clone(dp.DynamicValue.DefaultValue)
		dp.hasDefaultValue = true
	} else if dp.definition.DefaultValue != nil {
		dp.rawDefaultValue = slices.Clone(dp.definition.DefaultValue)
		dp.hasDefaultValue = true
	}

	if dp.DynamicValue.CastTemplate != "" {
		dp.tmpl, err = template.New("").
			Funcs(FuncMap()).
			Parse(dp.DynamicValue.CastTemplate)
		if err != nil {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("unable to render cast template").
					AddMeta("Error", err.Error()).
					AddMeta("DynamicParameterSetting", "cast_template"),
			)
			return warnings, nil
		}
	}

	if dp.definition.IsColumn {
		warnings = append(
			warnings,
			NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				SetMsg("column parameter cannot work in dynamic mode"),
		)
		return warnings, nil
	}

	columnIdx, column, ok := dp.driver.GetColumnByName(dp.DynamicValue.Column)
	if !ok {
		return ValidationWarnings{
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("column does not exist").
					AddMeta("DynamicParameterSetting", "column").
					AddMeta("ColumnName", dp.definition.Name),
			},
			nil
	}
	dp.column = column
	dp.columnIdx = columnIdx
	dp.tmplCtx = NewDynamicParameterContext(column)

	var castFuncDef *TypeCastDefinition
	if dp.DynamicValue.CastTo != "" {
		if dp.definition.LinkColumnParameter == "" {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("cast_to parameter is not supported for Non Linked transformer parameters").
					AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
					AddMeta("DynamicParameterSetting", "cast_to"),
			)
			return warnings, nil
		}

		castFuncDef, ok = CastFunctionsMap[dp.DynamicValue.CastTo]
		if !ok {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("unable to find cast_to function").
					AddMeta("CastToFuncName", dp.DynamicValue.CastTo).
					AddMeta("DynamicParameterSetting", "cast_to"),
			)
			return warnings, nil
		}
		dp.castToFunc = castFuncDef.Cast
	}

	if dp.definition.LinkColumnParameter != "" {
		param, ok := columnParameters[dp.definition.LinkColumnParameter]
		if !ok {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, dp.definition.LinkColumnParameter))
		}
		dp.linkedColumnParameter = param
		if !dp.linkedColumnParameter.definition.IsColumn {
			return nil, fmt.Errorf("linked parameter must be column: check transformer implementation")
		}
		dp.tmplCtx.setLinkedColumn(dp.linkedColumnParameter.Column)

		inputType := GetCanonicalTypeName(dp.driver, column.TypeName, uint32(column.TypeOid))
		outputType := GetCanonicalTypeName(dp.driver, dp.linkedColumnParameter.Column.TypeName, uint32(dp.linkedColumnParameter.Column.TypeOid))

		if castFuncDef != nil && !castFuncDef.ValidateTypes(inputType, outputType) {
			warnings = append(
				warnings,
				NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
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
			if dp.linkedColumnParameter.Column.TypeOid != column.TypeOid &&
				dp.linkedColumnParameter.definition.ColumnProperties != nil &&
				len(dp.linkedColumnParameter.definition.ColumnProperties.AllowedTypes) > 0 &&
				!IsTypeAllowedWithTypeMap(
					dp.driver,
					dp.linkedColumnParameter.definition.ColumnProperties.AllowedTypes,
					column.TypeName,
					column.TypeOid,
					true,
				) {
				warnings = append(warnings, NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					AddMeta("DynamicParameterSetting", "column").
					AddMeta("DynamicParameterColumnType", column.TypeName).
					AddMeta("DynamicParameterColumnName", column.Name).
					AddMeta("LinkedParameterName", dp.definition.LinkColumnParameter).
					AddMeta("LinkedColumnName", dp.linkedColumnParameter.Column.Name).
					AddMeta("LinkedColumnType", dp.linkedColumnParameter.Column.TypeName).
					AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
					SetMsg("linked parameter and dynamic parameter column name has different types"),
				)
			}

			if dp.definition.CastDbType != "" &&
				!IsTypeAllowedWithTypeMap(
					dp.driver,
					[]string{dp.definition.CastDbType},
					column.TypeName,
					column.TypeOid,
					true,
				) {
				warnings = append(warnings, NewValidationWarning().
					SetSeverity(ErrorValidationSeverity).
					SetMsg("unsupported column type: unsupported type according cast_db_type").
					AddMeta("DynamicParameterSetting", "column").
					AddMeta("DynamicParameterColumnType", column.TypeName).
					AddMeta("DynamicParameterColumnName", column.Name).
					AddMeta("CastDbType", dp.definition.CastDbType),
				)
			}
		}
	}

	return
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

	var usedDefaultValue bool

	if v.IsNull {
		if !dp.hasDefaultValue {
			return nil, fmt.Errorf("received NULL value from dynamic parameter")
		}
		usedDefaultValue = true
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

	if dp.tmpl != nil && !usedDefaultValue {
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
				Str("Error", err.Error()).
				Msg("error executing cast template")

			return nil, fmt.Errorf("error executing cast template: %w", err)
		}
		rawValue = dp.buf.Bytes()
	} else if dp.castToFunc != nil && !usedDefaultValue {
		rawValue, err = dp.castToFunc(dp.driver, rawValue)
		if err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
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
			Str("TableSchema", dp.driver.Table.Schema).
			Str("TableName", dp.driver.Table.Name).
			Msg("error decoding casted value")

		return nil, fmt.Errorf("error scanning casted value: %w", err)
	}
	return res, nil

}

func (dp *DynamicParameter) RawValue() (ParamsValue, error) {
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

		return ScanPointer(dp.defaultValueScanned, dest)
	}

	rawValue := v.Data

	var usedDefaultValue bool
	if dp.tmpl != nil && !usedDefaultValue {
		if err = dp.tmpl.Execute(dp.buf, dp.tmplCtx); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
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
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
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
		return ScanPointer(value, dest)
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
			Str("TableSchema", dp.driver.Table.Schema).
			Str("TableName", dp.driver.Table.Name).
			Msg("error decoding casted value")

		return fmt.Errorf("error scanning casted value: %w", err)
	}
	return nil
}
