package parameters

import (
	"text/template"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TODO:
// 	1. Decide On NULL behaviour - like raise error or use default
//  2.

type DynamicParameter struct {
	definition *toolkit.Parameter
	driver     *toolkit.Driver
	record     *toolkit.Record
	tmpl       *template.Template
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

func (p *DynamicParameter) Init(defs []*toolkit.Parameter, rawValue toolkit.ParamsValue) (toolkit.ValidationWarnings, error) {
	var warnings toolkit.ValidationWarnings

	if p.definition.IsColumn {
		warnings = append(
			warnings,
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("column parameter cannot work in dynamic mode").
				AddMeta("ParameterName", p.definition.Name),
		)
		return warnings, nil
	}

	// Algorithm
	// 1. If it has CastDbType check that type is the same as in CastDbType iof not - raise warning
	// 2. If it has linked parameter check that it has the same types otherwise raise validation error
	// 3.

}

func (p *DynamicParameter) Value() (value any, err error) {
	//TODO implement me
	panic("implement me")
}

func (p *DynamicParameter) RawValue() (rawValue toolkit.ParamsValue, err error) {
	//TODO implement me
	panic("implement me")
}

func (p *DynamicParameter) Scan(dest any) (empty bool, err error) {
	//TODO implement me
	panic("implement me")
}
