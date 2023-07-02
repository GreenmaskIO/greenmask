package transformers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const defaultPrecision int16 = 4

var RandomFloatTransformerMeta = TransformerMeta{
	Description: "Generate random float",
	ParamsDescription: map[string]string{
		"min":       "min value",
		"max":       "max value",
		"precision": "precision of the random value",
	},
	NewTransformer: NewRandomFloatTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar(float64(0)).
		SetSupportedOids(
			pgtype.Float4OID,
			pgtype.Float8OID,
		),
}

type RandomFloatTransformerParams struct {
	Min       float64 `mapstructure:"min" validate:"required"`
	Max       float64 `mapstructure:"max" validate:"required"`
	Precision int16   `mapstructure:"precision"`
	Nullable  bool    `mapstructure:"nullable"`
	Fraction  float32 `mapstructure:"fraction"`
}

type RandomFloatTransformer struct {
	TransformerBase
	RandomFloatTransformerParams
	precision float64
	rand      *rand.Rand
}

func NewRandomFloatTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := RandomFloatTransformerParams{
		Precision: defaultPrecision,
		Fraction:  DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	res := &RandomFloatTransformer{
		TransformerBase:              *base,
		RandomFloatTransformerParams: tParams,
		rand:                         rand.New(rand.NewSource(time.Now().UnixMicro())),
		precision:                    math.Pow(10, float64(tParams.Precision)),
	}

	return res, nil
}

func (gtt *RandomFloatTransformer) Transform(val string) (string, error) {
	if gtt.Nullable {
		if gtt.rand.Float32() < gtt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resFloat := gtt.Min + gtt.rand.Float64()*(gtt.Max-gtt.Min)
	resFloat = Round(resFloat, gtt.precision)
	res, err := gtt.EncodePlan.Encode(resFloat, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
