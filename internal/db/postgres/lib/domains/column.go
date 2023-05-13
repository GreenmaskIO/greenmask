package domains

import "github.com/wwoytenko/greenfuscator/internal/domains"

type Column struct {
	ColumnMeta    `json:"-" yaml:"-"`
	Name          string                    `mapstructure:"name" json:"name"`
	TransformConf domains.TransformerConfig `mapstructure:"transformer" json:"transformers" yaml:"transformers"`
	Transformer   domains.Transformer       `mapstructure:"-" json:"-" yaml:"-"`
}

type ColumnMeta struct {
	Type    string `yaml:"-"`
	NotNull bool   `yaml:"-"`
}
