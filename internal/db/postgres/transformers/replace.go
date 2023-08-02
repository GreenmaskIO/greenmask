package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const ReplaceTransformerName = "Replace"

var ReplaceTransformerMeta = TransformerMeta{
	Description: `Replace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"value": "replacing value",
	},
	NewTransformer: NewReplaceTransformer,
	Settings: NewTransformerSettings().
		SetCastVar("").
		SetVariadic().
		SetName(ReplaceTransformerName),
}

type ReplaceTransformerParams struct {
	Value    string  `mapstructure:"value" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type ReplaceTransformer struct {
	TransformerBase
	ReplaceTransformerParams
	value string
	rand  *rand.Rand
}

func NewReplaceTransformer(
	base *TransformerBase,
	params map[string]interface{},
) (domains.Transformer, error) {

	tParams := ReplaceTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	res := &ReplaceTransformer{
		TransformerBase:          *base,
		ReplaceTransformerParams: tParams,
		rand:                     rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	_, err := base.PgType.Codec.DecodeValue(base.TypeMap, uint32(base.Column.TypeOid), pgx.TextFormatCode, []byte(tParams.Value))
	if err != nil {
		return nil, fmt.Errorf("cannot decode value: %w", err)
	}
	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable at not null column")
	}

	return res, nil
}

func (rt *ReplaceTransformer) TransformAttr(val string) (string, error) {
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return rt.Value, nil
}

func (rt *ReplaceTransformer) Transform(data []byte) ([]byte, error) {

	record, attr, err := getColumnValueFromCsvRecord(data, rt.ColumnNum)
	if err != nil {
		return nil, fmt.Errorf("cannot parse csv record: %w", err)
	}

	transformedAttr, err := rt.TransformAttr(attr)
	if err != nil {
		return nil, err
	}

	return updateAttributeAndBuildRecord(record, transformedAttr, rt.ColumnNum)
}
