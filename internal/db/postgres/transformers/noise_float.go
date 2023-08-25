package transformers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/transformers/utils"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const NoiseFloatTransformerName = "NoiseFloat"

var NoiseFloatTransformerMeta = utils.TransformerMeta{
	Description: "Generate random float",
	ParamsDescription: map[string]string{
		"ratio":     "max random percentage for noise",
		"precision": "precision of the random value",
	},
	NewTransformer: NewNoiseFloatTransformer,
	Settings: utils.NewTransformerSettings().
		SetNullable().
		SetVariadic().
		SetCastVar(float64(0)).
		SetSupportedOids(
			pgtype.Float4OID,
			pgtype.Float8OID,
		).
		SetName(NoiseFloatTransformerName),
}

type NoiseFloatTransformerParams struct {
	Ratio     float64 `mapstructure:"ratio" validate:"required,min=0,max=1"`
	Precision int16   `mapstructure:"precision"`
	Nullable  bool    `mapstructure:"nullable"`
	Fraction  float32 `mapstructure:"fraction"`
}

type NoiseFloatTransformer struct {
	utils.TransformerBase
	NoiseFloatTransformerParams
	precision float64
	rand      *rand.Rand
	val       float64
}

func NewNoiseFloatTransformer(
	base *utils.TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := NoiseFloatTransformerParams{
		Precision: defaultPrecision,
		Fraction:  utils.DefaultNullFraction,
	}

	if err := utils.ParseTransformerParams(params, &tParams); err != nil {
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

func (nft *NoiseFloatTransformer) TransformAttr(val string) (string, error) {
	if val == utils.DefaultNullSeq {
		return val, nil
	}
	if err := nft.Scan(val, &nft.val); err != nil {
		return "", fmt.Errorf("cannot scan string into int64: %w", err)
	}

	if nft.Nullable {
		if nft.rand.Float32() < nft.Fraction {
			return utils.DefaultNullSeq, nil
		}
	}
	ratio := nft.rand.Float64() * nft.Ratio
	negative := nft.rand.Int63n(2) == 1
	if negative {
		ratio = ratio * -1
	}
	nft.val = round(nft.val+nft.val*ratio, nft.precision)
	res, err := nft.EncodePlan.Encode(nft.val, nil)
	if err != nil {
		return "", err
	}
	return string(res), err
}

func (nft *NoiseFloatTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := utils.GetColumnValueFromCsvRecord(nft.Table, data, nft.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := nft.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return utils.UpdateAttributeAndBuildRecord(nft.Table, record, transformedAttr, nft.ColumnNum)
}

func round(x, unit float64) float64 {
	return math.Floor(x*unit) / unit
}
