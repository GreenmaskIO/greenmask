package parameters

import (
	"fmt"
	"slices"
	"text/template"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TODO:
// 	1. Decide On NULL behaviour - like raise error or use default
//  2. You might need to move default value decoding to common functions

type DynamicParameter struct {
	definition            *toolkit.Parameter
	driver                *toolkit.Driver
	record                *toolkit.Record
	tmpl                  *template.Template
	dynamicValue          *toolkit.DynamicParamValue
	linkedColumnParameter *toolkit.Parameter
	columnIdx             int
}

func NewDynamicParameter(def *toolkit.Parameter, driver *toolkit.Driver) *DynamicParameter {
	return &DynamicParameter{
		definition: def,
		driver:     driver,
	}
}

func (p *DynamicParameter) SetRecord(r *toolkit.Record) {
	p.record = r
}

func (p *DynamicParameter) Init(defs []*toolkit.Parameter, dynamicSettings *toolkit.DynamicParamValue) (warnings toolkit.ValidationWarnings, err error) {

	// Algorithm
	// 1. If it has CastDbType check that type is the same as in CastDbType iof not - raise warning
	// 2. If it has linked parameter check that it has the same types otherwise raise validation error

	if dynamicSettings == nil {
		panic("dynamicSettings is nil")
	}

	if dynamicSettings.Column == "" {
		warnings = append(
			warnings,
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("received empty \"column\" parameter").
				AddMeta("DynamicParameterSetting", "column"),
		)
		return warnings, nil
	}

	if dynamicSettings.CastTemplate != "" {
		p.tmpl, err = template.New("").Parse(dynamicSettings.CastTemplate)
		if err != nil {
			warnings = append(
				warnings,
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					SetMsg("unable to render cast template").
					AddMeta("Error", err.Error()).
					AddMeta("DynamicParameterSetting", "cast_template"),
			)
			return warnings, nil
		}
	}

	if p.definition.IsColumn {
		warnings = append(
			warnings,
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("column parameter cannot work in dynamic mode"),
		)
		return warnings, nil
	}

	columnIdx, column, ok := p.driver.GetColumnByName(dynamicSettings.Column)
	if !ok {
		return toolkit.ValidationWarnings{
				toolkit.NewValidationWarning().
					SetSeverity(toolkit.ErrorValidationSeverity).
					SetMsg("column does not exist").
					AddMeta("DynamicParameterSetting", "column").
					AddMeta("ColumnName", p.definition.Name),
			},
			nil
	}
	p.columnIdx = columnIdx

	if p.definition.LinkColumnParameter != "" {
		paramIdx := slices.IndexFunc(defs, func(def *toolkit.Parameter) bool {
			return def.Name == p.definition.LinkColumnParameter
		})
		if paramIdx == -1 {
			panic(fmt.Sprintf(`parameter with name "%s" is not found`, p.definition.LinkColumnParameter))
		}
		p.linkedColumnParameter = defs[paramIdx]
		if !p.linkedColumnParameter.IsColumn {
			return nil, fmt.Errorf("linked parameter must be column: check transformer implementation")
		}

		var linkedColumnName string
		// TODO: You have to replace defs it to parameter value instead of defs since you have to get the column
		// 	value from static parameter
		_, err := p.linkedColumnParameter.Scan(&linkedColumnName)
		if err != nil {
			return nil, fmt.Errorf("error scanning linked parameter value: %w", err)
		}
		_, linkedColumn, ok := p.driver.GetColumnByName(dynamicSettings.Column)
		if !ok {
			panic(fmt.Sprintf("column with name \"%s\" is not found", linkedColumnName))
		}

		// TODO: Recheck this cond since some of types implicitly literally equal
		// TODO: There is bug with column overriding type since OverriddenTypeOid is not checking
		if linkedColumn.TypeOid != column.TypeOid && p.tmpl == nil {
			warnings = append(warnings, toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("DynamicParameterSetting", "column").
				AddMeta("DynamicParameterColumnType", column.TypeName).
				AddMeta("DynamicParameterColumnName", column.Name).
				AddMeta("LinkedParameterName", p.definition.LinkColumnParameter).
				AddMeta("LinkedColumnName", linkedColumnName).
				AddMeta("LinkedColumnType", linkedColumn.TypeName).
				AddMeta("Hint", "you can use \"cast_template\" for casting value to supported type").
				SetMsg("linked parameter and dynamic parameter column name has different types"),
			)
		}
	}

	if p.definition.CastDbType != "" &&
		!toolkit.IsTypeAllowed(
			[]string{p.definition.CastDbType},
			p.driver.CustomTypes,
			column.Name,
			true,
		) {
		warnings = append(warnings, toolkit.NewValidationWarning().
			SetSeverity(toolkit.ErrorValidationSeverity).
			SetMsg("unsupported column type: unsupported type according cast_db_type").
			AddMeta("DynamicParameterSetting", "column").
			AddMeta("DynamicParameterColumnType", column.TypeName).
			AddMeta("DynamicParameterColumnName", column.Name).
			AddMeta("CastDbType", p.definition.CastDbType),
		)

		return warnings, nil
	}

	return
}

func (p *DynamicParameter) Value() (value any, err error) {
	// TODO: Add logic for using cst template and null behaviour
	v, err := p.record.GetColumnValueByIdx(p.columnIdx)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

func (p *DynamicParameter) RawValue() (rawValue toolkit.ParamsValue, err error) {
	// TODO: Add logic for using cst template and null behaviour
	v, err := p.record.GetRawColumnValueByIdx(p.columnIdx)
	if err != nil {
		return nil, err
	}
	return v.Data, nil
}

func (p *DynamicParameter) Scan(dest any) (bool, error) {
	// TODO: Add logic for using cst template and null behaviour
	empty, err := p.record.ScanColumnValueByIdx(p.columnIdx, dest)
	if err != nil {
		return true, err
	}
	return empty, nil
}
