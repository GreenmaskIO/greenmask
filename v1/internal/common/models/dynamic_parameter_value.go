package models

type DynamicParamValue struct {
	Column       string
	CastTo       string
	Template     string
	DefaultValue ParamsValue
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
