package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var RandomIntTransformerMeta = TransformerMeta{
	Description: "Generate random int",
	ParamsDescription: map[string]string{
		"min": "min value",
		"max": "max value",
	},
	SupportedTypeOids: []int{
		pgtype.Int2OID,
		pgtype.Int4OID,
		pgtype.Int8OID,
	},
	//NewTransformer: NewRandomIntTransformerV2,
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

func NewRandomIntTransformerV2(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(column, typeMap, useType, RandomIntTransformerMeta.SupportedTypeOids, int64(1))
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := RandomIntTransformerParams{
		Fraction: 0.3,
	}

	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
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

//100, 1000 = 1000 - 100
//-100, 1000 = 1000 -(-100)
