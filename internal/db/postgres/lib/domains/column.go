package domains

import "github.com/wwoytenko/greenfuscator/internal/domains"

type Column struct {
	Name string `mapstructure:"name"`
	ColumnMeta
	TransformConf domains.TransformerConfig `mapstructure:"transformer"`
	Transformer   domains.Transformer       `mapstructure:"-"`
}

type ColumnMeta struct {
	Type    string `yaml:"-"`
	NotNull bool   `yaml:"-"`
}
