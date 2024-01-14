package toolkit

import (
	"fmt"
	"text/template"
)

// TODO:
// 	1. Decide On NULL behaviour - like raise error or use default
//  2. You might need to move default value decoding to common functions

type DynamicParameter struct {
	DynamicValue          *DynamicParamValue
	definition            *ParameterDefinition
	driver                *Driver
	record                *Record
	tmpl                  *template.Template
	linkedColumnParameter *StaticParameter
	columnIdx             int
}

func NewDynamicParameter(def *ParameterDefinition, driver *Driver) *DynamicParameter {
	return &DynamicParameter{
		definition: def,
		driver:     driver,
	}
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

		var linkedColumnName string
		// TODO: You have to replace defs it to parameter value instead of defs since you have to get the column
		// 	value from static parameter
		_, err := dp.linkedColumnParameter.Scan(&linkedColumnName)
		if err != nil {
			return nil, fmt.Errorf("error scanning linked parameter value: %w", err)
		}
		_, linkedColumn, ok := dp.driver.GetColumnByName(linkedColumnName)
		if !ok {
			panic(fmt.Sprintf("column with name \"%s\" is not found", linkedColumnName))
		}

		// TODO: Recheck this cond since some of types implicitly literally equal for instance TIMESTAMP and TIMESTAMPTZ
		// TODO: There is bug with column overriding type since OverriddenTypeOid is not checking
		if linkedColumn.TypeOid != column.TypeOid && dp.tmpl == nil {
			warnings = append(warnings, NewValidationWarning().
				SetSeverity(ErrorValidationSeverity).
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("DynamicParameterColumnType", column.TypeName).
				AddMeta("DynamicParameterColumnName", column.Name).
				AddMeta("LinkedParameterName", dp.definition.LinkColumnParameter).
				AddMeta("LinkedColumnName", linkedColumnName).
				AddMeta("LinkedColumnType", linkedColumn.TypeName).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				SetMsg("linked parameter and dynamic parameter column name has different types"),
			)
		}
	}

	if dp.definition.CastDbType != "" &&
		!IsTypeAllowed(
			[]string{dp.definition.CastDbType},
			dp.driver.CustomTypes,
			column.Name,
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

		return warnings, nil
	}

	return
}

func (dp *DynamicParameter) Value() (value any, err error) {
	// TODO: Add logic for using cst template and null behaviour
	v, err := dp.record.GetColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

func (dp *DynamicParameter) RawValue() (rawValue ParamsValue, err error) {
	// TODO: Add logic for using cst template and null behaviour
	v, err := dp.record.GetRawColumnValueByIdx(dp.columnIdx)
	if err != nil {
		return nil, err
	}
	return v.Data, nil
}

func (dp *DynamicParameter) Scan(dest any) (bool, error) {
	// TODO: Add logic for using cst template and null behaviour
	empty, err := dp.record.ScanColumnValueByIdx(dp.columnIdx, dest)
	if err != nil {
		return true, err
	}
	return empty, nil
}
