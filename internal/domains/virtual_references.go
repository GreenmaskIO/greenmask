package domains

type ReferencedColumn struct {
	Name       string `mapstructure:"name" json:"name" yaml:"name"`
	Expression string `mapstructure:"expression" json:"expression" yaml:"expression"`
}

type Reference struct {
	Schema  string              `mapstructure:"schema" json:"schema" yaml:"schema"`
	Name    string              `mapstructure:"name" json:"name" yaml:"name"`
	NotNull bool                `mapstructure:"not_null" json:"not_null" yaml:"not_null"`
	Columns []*ReferencedColumn `mapstructure:"columns" json:"columns" yaml:"columns"`
}

type VirtualReference struct {
	Schema     string       `mapstructure:"schema" json:"schema" yaml:"schema"`
	Name       string       `mapstructure:"name" json:"name" yaml:"name"`
	References []*Reference `mapstructure:"references" json:"references" yaml:"references"`
}
