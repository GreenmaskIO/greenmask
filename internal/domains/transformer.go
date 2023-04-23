package domains

type Transformer interface {
	Transform(originalValue string) (string, error)
}

type TransformerConfig struct {
	Name   string            `yaml:"name"`
	Params map[string]string `yaml:"params"`
}
