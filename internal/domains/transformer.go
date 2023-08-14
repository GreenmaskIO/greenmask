package domains

type TransformationType int

const (
	AttributeTransformation = iota
	TupleTransformation
	ShiftTransformation
	BlendTransformation
)

type Transformer interface {
	Transform([]byte) ([]byte, error)
	Validate() (ValidationWarnings, error)
	IsCustom() bool
	GetName() string
	GetTransformationType() TransformationType
	GetParam(name string) (interface{}, bool)
}

type TransformerConfig struct {
	Name   string                 `mapstructure:"name"`
	Params map[string]interface{} `mapstructure:"params"`
}
