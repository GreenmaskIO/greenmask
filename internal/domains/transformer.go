package domains

type Transformer interface {
	Transform(originalValue string) (string, error)
	Validate() RuntimeErrors
}

type TransformerConfig struct {
	Name   string                 `mapstructure:"name"`
	Params map[string]interface{} `mapstructure:"params"`
}
