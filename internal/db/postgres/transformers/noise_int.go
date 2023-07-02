package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var NoiseIntTransformerMeta = TransformerMeta{
	Description: "Make noise value for int",
	ParamsDescription: map[string]string{
		"ratio": "max random percentage for noise",
	},
	NewTransformer: NewNoiseIntTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar(int64(1)).
		SetSupportedOids(
			pgtype.Int2OID,
			pgtype.Int4OID,
			pgtype.Int8OID,
		),
}

type NoiseIntTransformerParams struct {
	Ratio    float64 `mapstructure:"ratio" validate:"required,min=0,max=1"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction,min=0,max=1"`
}

type NoiseIntTransformer struct {
	TransformerBase
	NoiseIntTransformerParams
	rand *rand.Rand
	val  int64
}

func NewNoiseIntTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {
	tParams := NoiseIntTransformerParams{
		Fraction: DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	res := &NoiseIntTransformer{
		TransformerBase:           *base,
		NoiseIntTransformerParams: tParams,
		rand:                      rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	return res, nil

}

func (gtt *NoiseIntTransformer) Transform(val string) (string, error) {

	if val == DefaultNullSeq {
		return val, nil
	}
	if err := gtt.Scan(val, &gtt.val); err != nil {
		return "", fmt.Errorf("cannot scan string into int64: %w", err)
	}

	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	ratio := gtt.rand.Float64() * gtt.Ratio
	negative := gtt.rand.Int63n(2) == 1
	if negative {
		ratio = ratio * -1
	}
	res, err := gtt.EncodePlan.Encode(gtt.val+int64(float64(gtt.val)*ratio), nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
