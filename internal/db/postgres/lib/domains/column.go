package domains

import "github.com/wwoytenko/greenfuscator/internal/domains"

type AttNum int

type Column struct {
	ColumnMeta    `json:"-" yaml:"-"`
	Name          string                    `mapstructure:"name" json:"name"`
	TransformConf domains.TransformerConfig `mapstructure:"transformer" json:"transformers" yaml:"transformers"`
	Transformer   domains.Transformer       `mapstructure:"-" json:"-" yaml:"-"`
}

type ColumnMeta struct {
	Num     AttNum `json:"-" yaml:"-"`
	TypeOid Oid    `json:"-" yaml:"-"`
	NotNull bool   `json:"-" yaml:"-"`
	Length  int64  `json:"-" yaml:"-"`
}
