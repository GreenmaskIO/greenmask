package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var RandomBoolTransformerSupportedOids = []int{
	pgtype.BoolOID,
}

var RandomBoolTransformerMeta = TransformerMeta{
	Description:       "Generate random bool",
	SupportedTypeOids: RandomBoolTransformerSupportedOids,
	NewTransformer:    NewRandomBoolTransformer,
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
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(column, typeMap, useType, RandomBoolTransformerSupportedOids, true)
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := RandomBoolTransformerParams{
		Fraction: 0.3,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable on not null column")
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
