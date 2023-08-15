package transformers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	defaultPrecision           int16 = 4
	RandomFloatTransformerName       = "RandomFloat"
)

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
		).
		SetName(RandomFloatTransformerName),
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

func (rft *RandomFloatTransformer) TransformAttr(val string) (string, error) {
	if rft.Nullable {
		if rft.rand.Float32() < rft.Fraction {
			return DefaultNullSeq, nil
		}
	}
	resFloat := rft.Min + rft.rand.Float64()*(rft.Max-rft.Min)
	resFloat = Round(resFloat, rft.precision)
	res, err := rft.EncodePlan.Encode(resFloat, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func (rft *RandomFloatTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(rft.Table, data, rft.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rft.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(rft.Table, record, transformedAttr, rft.ColumnNum)
}
