package transformers

import (
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var SetNullTransformerMeta = TransformerMeta{
	Description:    `Set NULL value`,
	NewTransformer: NewSetNullTransformer,
	Settings: NewTransformerSettings().
		SetCastVar("").
		SetNullable(),
}

type SetNullTransformer struct {
	TransformerBase
	nullSequence string
}

func NewSetNullTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {
	// We're always setting null
	if params == nil {
		params = make(map[string]interface{})
	}
	params["nullable"] = true

	return &SetNullTransformer{
		TransformerBase: *base,
		nullSequence:    DefaultNullSeq,
	}, nil
}

func (rt *SetNullTransformer) Transform(val string) (string, error) {
	return rt.nullSequence, nil
}
