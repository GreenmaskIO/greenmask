package domains

type Column struct {
	Name      string `yaml:"name"`
	Type      string
	Transform Transformer `yaml:"transformer"`
	NotNull   bool
}
