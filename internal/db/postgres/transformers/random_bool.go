package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const RandomBoolTransformerName = "RandomBool"

var RandomBoolTransformerMeta = TransformerMeta{
	Description:    "Generate random bool",
	NewTransformer: NewRandomBoolTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetCastVar(true).
		SetSupportedOids(
			pgtype.BoolOID,
		).
		SetName(RandomBoolTransformerName),
}

type RandomBoolTransformerParams struct {
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomBoolTransformer struct {
	TransformerBase
	RandomBoolTransformerParams
	rand *rand.Rand
}

func NewRandomBoolTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := RandomBoolTransformerParams{
		Fraction: 0.3,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	res := &RandomBoolTransformer{
		TransformerBase:             *base,
		RandomBoolTransformerParams: tParams,
		rand:                        rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	return res, nil

}

func (rbt *RandomBoolTransformer) TransformAttr(val string) (string, error) {
	if rbt.Nullable {
		if rbt.rand.Float32() < rbt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	res, err := rbt.EncodePlan.Encode(rbt.rand.Int63n(2) == 1, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func (rbt *RandomBoolTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(data, rbt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rbt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(record, transformedAttr, rbt.ColumnNum)
}
