package transformers

import (
	"errors"

	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type ReplaceTransformer struct {
	Column   pgDomains.ColumnMeta
	newValue string
}

func NewReplaceTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	val, ok := params["value"]
	if !ok {
		return nil, errors.New("expected value key")
	}
	return &ReplaceTransformer{
		Column:   column,
		newValue: val,
	}, nil
}

func (rt *ReplaceTransformer) Transform(val string) (string, error) {
	return rt.newValue, nil
}
