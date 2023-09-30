package transformers

type CustomTransformerDefinition struct {
	Name        string       `mapstructure:"name" yaml:"name" json:"name"`
	Description string       `mapstructure:"description" yaml:"description" json:"description"`
	Executable  string       `mapstructure:"executable" yaml:"executable" json:"executable"`
	Args        []string     `mapstructure:"args" yaml:"args" json:"args"`
	Parameters  []*Parameter `mapstructure:"parameters" yaml:"parameters" json:"parameters"`
}
