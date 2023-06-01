package transformers

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var ReplaceTransformerSupportedOids = []int{
	AnyOid,
}

var ReplaceTransformerMeta = TransformerMeta{
	Description: `Replace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"value": "replacing value",
	},
	SupportedTypeOids: ReplaceTransformerSupportedOids,
	NewTransformer:    NewReplaceTransformer,
}

type ReplaceTransformerParams struct {
	Value    string  `mapstructure:"value" validate:"required"`
	Nullable bool    `mapstructure:"nullable"`
	Fraction float32 `mapstructure:"fraction"`
}

type ReplaceTransformer struct {
	TransformerBase
	ReplaceTransformerParams
	Column pgDomains.ColumnMeta
	value  string
	rand   *rand.Rand
}

func NewReplaceTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	base, err := NewTransformerBase(column, typeMap, useType, ReplaceTransformerSupportedOids, "")
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	tParams := ReplaceTransformerParams{
		Fraction: DefaultNullFraction,
	}
	if err := parseTransformerParams(params, &tParams); err != nil {
		return nil, fmt.Errorf("parameters parsing error: %w", err)
	}

	res := &ReplaceTransformer{
		TransformerBase:          *base,
		ReplaceTransformerParams: tParams,
		Column:                   column,
		rand:                     rand.New(rand.NewSource(time.Now().UnixMicro())),
	}

	_, err = base.PgType.Codec.DecodeValue(typeMap, column.TypeOid, pgx.TextFormatCode, []byte(tParams.Value))
	if err != nil {
		return nil, fmt.Errorf("cannot decode value: %w", err)
	}
	if tParams.Nullable && base.Column.NotNull {
		return nil, fmt.Errorf("transformer cannot be nullable on not null column")
	}

	return res, nil
}

func (rt *ReplaceTransformer) Transform(val string) (string, error) {
	if rt.Nullable {
		if rt.rand.Float32() < rt.Fraction {
			return DefaultNullSeq, nil
		}
	}
	return rt.Value, nil
}
