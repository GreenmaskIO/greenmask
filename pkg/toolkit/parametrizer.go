package toolkit

type Parameterizer interface {
	Value() (value any, err error)
	RawValue() (rawValue ParamsValue, err error)
	Scan(dest any) (empty bool, err error)
	GetDefinition() *ParameterDefinition
}
