package domains

type Transformer interface {
	Transform(originalValue string) (string, error)
}

type TransformerConfig struct {
	Name   string            `mapstructure:"name"`
	Params map[string]string `mapstructure:"params"`
}
