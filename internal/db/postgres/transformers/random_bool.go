package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var RandomBoolTransformerMeta = TransformerMeta{
	Description:    "Generate random bool",
	NewTransformer: NewRandomBoolTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetCastVar(true).
		SetSupportedOids(
			pgtype.BoolOID,
		),
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

func (gtt *RandomBoolTransformer) Transform(val string) (string, error) {
	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	res, err := gtt.EncodePlan.Encode(gtt.rand.Int63n(2) == 1, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
