package domains

type Column struct {
	Name string `mapstructure:"name"`
	ColumnMeta
	TransformConf TransformerConfig `mapstructure:"transformer"`
	Transformer   Transformer       `mapstructure:"-"`
}

type ColumnMeta struct {
	Type    string `yaml:"-"`
	NotNull bool   `yaml:"-"`
}
