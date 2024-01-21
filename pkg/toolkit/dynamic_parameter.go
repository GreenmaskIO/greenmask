package toolkit

import (
	"bytes"
	"fmt"
	"slices"
	"text/template"

	"github.com/rs/zerolog/log"
)

// TODO:
// 	1. Decide On NULL behaviour - like raise error or use default
//  2. You might need to move default value decoding to common functions

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
	//defaultValueFromDynamicParamValue any
	//defaultValueFromDefinition        any

	hasDefaultValue bool
	defaultValue    any
	rawDefaultValue ParamsValue
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
}

func (dp *DynamicParameter) Init(columnParameters map[string]*StaticParameter, dynamicValue *DynamicParamValue) (warnings ValidationWarnings, err error) {

	// Algorithm
	// 1. If it has CastDbType check that type is the same as in CastDbType iof not - raise warning
	// 2. If it has linked parameter check that it has the same types otherwise raise validation error

	if !dp.definition.DynamicModeSupport {
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

	if dynamicValue.Column == "" {
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
		dp.tmpl, err = template.New("").Parse(dp.DynamicValue.CastTemplate)
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
	dp.columnIdx = columnIdx

	if dp.definition.LinkColumnParameter != "" {
		param, ok := columnParameters[dp.definition.LinkColumnParameter]
		if !ok {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, dp.definition.LinkColumnParameter))
		}
		dp.linkedColumnParameter = param
		if !dp.linkedColumnParameter.definition.IsColumn {
			return nil, fmt.Errorf("linked parameter must be column: check transformer implementation")
		}

		// TODO: There is bug with column overriding type since OverriddenTypeOid is not checking
		// TODO: Add CompatibleTypes checking there. Consider IsTypeAllowedWithTypeMap usage
		if dp.tmpl == nil {
			// Check that column parameter has the same type with dynamic parameter value or at least dynamic parameter
			// column is compatible with type in the list. This logic is controversial since it might be unexpected
			// when dynamic param column has different though compatible types. Consider it
			if dp.linkedColumnParameter.Column.TypeOid != column.TypeOid && (dp.definition.ColumnProperties != nil &&
				!IsTypeAllowedWithTypeMap(
					dp.driver,
					dp.definition.ColumnProperties.AllowedTypes,
					column.TypeName,
					column.TypeOid,
					true,
				)) {
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
	if dp.record == nil {
		return nil, fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	}

	rawValue, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("erro getting raw column value: %w", err)
	}

	if rawValue.IsNull {
		if dp.hasDefaultValue {
			return nil, fmt.Errorf("IMPLEMENT ME")
		}
		return nil, fmt.Errorf("received NULL value from dynamic parameter")
	}

	if dp.tmpl != nil {
		if err = dp.tmpl.Execute(dp.buf, nil); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(rawValue.Data)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
				Msg("error executing cast template")

			return nil, fmt.Errorf("error executing cast template: %w", err)
		}
		castedValue := dp.buf.Bytes()
		res, err := dp.driver.DecodeValueByColumnIdx(dp.columnIdx, castedValue)
		if err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(rawValue.Data)).
				Str("CastedValue", string(castedValue)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
				Msg("error decoding casted value")

			return nil, fmt.Errorf("error scanning casted value: %w", err)
		}
		return res, nil
	}

	res, err := dp.record.GetColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("error scanning value: %w", err)
	}
	return res.Value, nil

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
			return nil, fmt.Errorf("IMPLEMENT ME")
		}
		return nil, fmt.Errorf("received NULL value from dynamic parameter")
	}
	return rawValue.Data, nil
}

func (dp *DynamicParameter) Scan(dest any) error {
	if dp.record == nil {
		return fmt.Errorf("check transformer implementation: dynamic parameter usage during initialization stage is prohibited")
	}

	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return fmt.Errorf("erro getting raw column value: %w", err)
	}

	if v.IsNull {
		var value any
		if dp.hasDefaultValue {
			return fmt.Errorf("IMPLEMENT ME")
		} else {
			return fmt.Errorf("received NULL value from dynamic parameter")
		}
		if err = ScanPointer(dest, value); err != nil {
			return fmt.Errorf("error scanning default value: %w", err)
		}
		return nil
	}

	if dp.tmpl != nil {
		if err = dp.tmpl.Execute(dp.buf, nil); err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
				Msg("error executing cast template")

			return fmt.Errorf("error executing cast template: %w", err)
		}
		castedValue := dp.buf.Bytes()
		err := dp.driver.ScanValueByColumnIdx(dp.columnIdx, castedValue, dest)
		if err != nil {
			log.Debug().
				Err(err).
				Str("ParameterName", dp.definition.Name).
				Str("RawValue", string(v.Data)).
				Str("CastedValue", string(castedValue)).
				Str("TableSchema", dp.driver.Table.Schema).
				Str("TableName", dp.driver.Table.Name).
				Msg("error decoding casted value")

			return fmt.Errorf("error scanning casted value: %w", err)
		}
		return nil
	}

	_, err = dp.record.ScanColumnValueByIdx(dp.columnIdx, dest)
	if err != nil {
		return fmt.Errorf("error scanning value: %w", err)
	}
	return nil
}
