package transformers

import (
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var SetNullTransformerMeta = TransformerMeta{
	Description: `Set NULL value`,
	ParamsDescription: map[string]string{
		"nullSequence": "null sequence for COPY command (default \\N)",
	},
	SupportedTypeOids: []int{
		AnyOid,
	},
	NewTransformer: NewSetNullTransformer,
}

type SetNullTransformer struct {
	Column       pgDomains.ColumnMeta
	nullSequence string
}

func NewSetNullTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	nullSequence, ok := params["nullSequence"]
	if !ok {
		nullSequence = DefaultNullSeq
	}

	return &SetNullTransformer{
		Column:       column,
		nullSequence: nullSequence,
	}, nil
}

func (rt *SetNullTransformer) Transform(val string) (string, error) {
	return rt.nullSequence, nil
}
