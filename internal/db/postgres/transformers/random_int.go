package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

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
		),
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

func (gtt *RandomIntTransformer) Transform(val string) (string, error) {

	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resInt := gtt.rand.Int63n(gtt.Max-gtt.Min) + gtt.Min
	res, err := gtt.EncodePlan.Encode(resInt, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
