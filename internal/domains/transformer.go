package domains

type TransformationType int

const (
	AttributeTransformation = iota
	TupleTransformation
)

type Transformer interface {
	Transform(originalValue string) (string, error)
	Validate() RuntimeErrors
	IsCustom() bool
}

type TransformerConfig struct {
	Name   string                 `mapstructure:"name"`
	Params map[string]interface{} `mapstructure:"params"`
}
