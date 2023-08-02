package transformers

import (
	"fmt"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const SetNullTransformerName = "SetNull"

var SetNullTransformerMeta = TransformerMeta{
	Description:    `Set NULL value`,
	NewTransformer: NewSetNullTransformer,
	Settings: NewTransformerSettings().
		SetCastVar("").
		SetNullable().
		SetName(SetNullTransformerName),
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

func (snt *SetNullTransformer) TransformAttr(val string) (string, error) {
	return snt.nullSequence, nil
}

func (snt *SetNullTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(data, snt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := snt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(record, transformedAttr, snt.ColumnNum)
}
