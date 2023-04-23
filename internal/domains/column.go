package domains

type Column struct {
	Name string `yaml:"name"`
	ColumnMeta
	TransformConf TransformerConfig `yaml:"transformer"`
	Transformer   Transformer       `yaml:"-"`
}

type ColumnMeta struct {
	Type    string `yaml:"-"`
	NotNull bool   `yaml:"-"`
}
