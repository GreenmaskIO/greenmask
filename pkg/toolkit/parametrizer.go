package toolkit

type Parameterizer interface {
	Value() (value any, err error)
	RawValue() (rawValue ParamsValue, err error)
	Scan(dest any) (err error)
	GetDefinition() *ParameterDefinition
	IsDynamic() bool
	IsEmpty() (bool, error)
}
