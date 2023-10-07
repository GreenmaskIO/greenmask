package toolkit

type Definition struct {
	Name             string             `json:"name"`
	Description      string             `json:"description"`
	Parameters       []*Parameter       `json:"parameters"`
	Validate         bool               `json:"validate"`
	ExpectedExitCode int                `json:"expected_exit_code"`
	New              NewTransformerFunc `json:"-"`
}

func NewDefinition(name string, makeFunc NewTransformerFunc) *Definition {
	return &Definition{
		Name: name,
		New:  makeFunc,
	}
}

func (d *Definition) SetDescription(v string) *Definition {
	d.Description = v
	return d
}

func (d *Definition) AddParameter(v *Parameter) *Definition {
	if v == nil {
		panic("parameter is nil")
	}
	d.Parameters = append(d.Parameters, v)
	return d
}

func (d *Definition) SetValidate(v bool) *Definition {
	d.Validate = v
	return d
}

func (d *Definition) SetExpectedExitCode(v int) *Definition {
	d.ExpectedExitCode = v
	return d
}
