package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const RandomIntTransformerName = "RandomInt"

var RandomIntTransformerMeta = TransformerMeta{
	Description: "Generate random int",
	ParamsDescription: map[string]string{
		"min": "min value",
		"max": "max value",
	},
	NewTransformer: NewRandomIntTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar(int64(0)).
		SetSupportedOids(
			pgtype.Int2OID,
			pgtype.Int4OID,
			pgtype.Int8OID,
		).
		SetName(RandomIntTransformerName),
}

type RandomIntTransformerParams struct {
	Min      int64   `mapstructure:"min" validate:"required"`
	Max      int64   `mapstructure:"max" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type RandomIntTransformer struct {
	TransformerBase
	RandomIntTransformerParams
	rand *rand.Rand
}

func NewRandomIntTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := RandomIntTransformerParams{
		Fraction: DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	res := &RandomIntTransformer{
		TransformerBase:            *base,
		RandomIntTransformerParams: tParams,
		rand:                       rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	return res, nil

}

func (rit *RandomIntTransformer) TransformAttr(val string) (string, error) {

	if rit.Nullable {
		if rit.rand.Float32() < rit.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resInt := rit.rand.Int63n(rit.Max-rit.Min) + rit.Min
	res, err := rit.EncodePlan.Encode(resInt, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func (rit *RandomIntTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(data, rit.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rit.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(record, transformedAttr, rit.ColumnNum)
}
