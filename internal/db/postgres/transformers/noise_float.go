package transformers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var NoiseFloatTransformerMeta = TransformerMeta{
	Description: "Generate random float",
	ParamsDescription: map[string]string{
		"ratio":     "max random percentage for noise",
		"precision": "precision of the random value",
	},
	NewTransformer: NewNoiseFloatTransformer,
	Settings: NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar(float64(0)).
		SetSupportedOids(
			pgtype.Float4OID,
			pgtype.Float8OID,
		),
}

type NoiseFloatTransformerParams struct {
	Ratio     float64 `mapstructure:"ratio" validate:"required,min=0,max=1"`
	Precision int16   `mapstructure:"precision"`
	Nullable  bool    `mapstructure:"nullable"`
	Fraction  float32 `mapstructure:"fraction"`
}

type NoiseFloatTransformer struct {
	TransformerBase
	NoiseFloatTransformerParams
	precision float64
	rand      *rand.Rand
	val       float64
}

func NewNoiseFloatTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := NoiseFloatTransformerParams{
		Precision: defaultPrecision,
		Fraction:  DefaultNullFraction,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	res := &NoiseFloatTransformer{
		TransformerBase:             *base,
		NoiseFloatTransformerParams: tParams,
		rand:                        rand.New(rand.NewSource(time.Now().UnixMicro())),
		precision:                   math.Pow(10, float64(tParams.Precision)),
	}

	return res, nil
}

func (gtt *NoiseFloatTransformer) Transform(val string) (string, error) {
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
	gtt.val = Round(gtt.val+gtt.val*ratio, gtt.precision)
	res, err := gtt.EncodePlan.Encode(gtt.val, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}
