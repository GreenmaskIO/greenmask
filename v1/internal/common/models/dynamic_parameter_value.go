package models

type DynamicParamValue struct {
	Column       string      `json:"column"`
	CastTo       string      `json:"cast_to,omitempty"`
	Template     string      `json:"template,omitempty"`
	DefaultValue ParamsValue `json:"default_value,omitempty"`
}

func NewDynamicParamValue(
	column string,
	castTo string,
	template string,
	defaultValue ParamsValue,
) DynamicParamValue {
	return DynamicParamValue{
		Column:       column,
		CastTo:       castTo,
		Template:     template,
		DefaultValue: defaultValue,
	}
}
