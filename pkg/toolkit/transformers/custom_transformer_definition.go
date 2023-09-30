package transformers

type CustomTransformerDefinition struct {
	Name        string       `mapstructure:"name" yaml:"name"`
	Description string       `mapstructure:"description" yaml:"description"`
	Executable  string       `mapstructure:"executable" yaml:"executable"`
	Args        []string     `mapstructure:"args" yaml:"args"`
	Parameters  []*Parameter `mapstructure:"parameters" yaml:"parameters"`
}
