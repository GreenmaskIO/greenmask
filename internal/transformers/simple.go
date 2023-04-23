package transformers

import (
	"errors"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type ReplaceTransformer struct {
	Column   domains.ColumnMeta
	newValue string
}

func NewReplaceTransformer(column domains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	val, ok := params["value"]
	if !ok {
		return nil, errors.New("expected value key")
	}
	return &ReplaceTransformer{
		Column:   column,
		newValue: val,
	}, nil
}

func (rt *ReplaceTransformer) Transform(val string) (string, error) {
	return rt.newValue, nil
}
