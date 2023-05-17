package transformers

import (
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var defaultNullSeq = "\\N"

type SetNullTransformer struct {
	Column pgDomains.ColumnMeta
}

func NewSetNullTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	return &SetNullTransformer{
		Column: column,
	}, nil
}

func (rt *SetNullTransformer) Transform(val string) (string, error) {
	return defaultNullSeq, nil
}
