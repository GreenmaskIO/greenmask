package transformers

type CustomTransformerDefinition struct {
	Name         string       `mapstructure:"name" yaml:"name" json:"name"`
	Description  string       `mapstructure:"description" yaml:"description" json:"description"`
	Executable   string       `mapstructure:"executable" yaml:"executable" json:"executable"`
	Args         []string     `mapstructure:"args" yaml:"args" json:"args"`
	Parameters   []*Parameter `mapstructure:"parameters" yaml:"parameters" json:"parameters"`
	Validate     bool         `mapstructure:"validate" yaml:"validate" json:"validate"`
	AutoDiscover bool         `mapstructure:"auto_discover" yaml:"auto_discover" json:"auto_discover"`
}
