package domains

type Transformer struct {
	Name        string            `yaml:"name"`
	Params      map[string]string `yaml:"params"`
	Transformer TransformerFunc
}

type TransformerFunc func(val string, params map[string]string) (string, error)
