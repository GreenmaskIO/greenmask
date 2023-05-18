package transformers

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type ReplaceTransformer struct {
	Column   pgDomains.ColumnMeta
	newValue string
}

func NewReplaceTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var cast string
	val, ok := params["value"]
	if !ok {
		return nil, errors.New("expected value key")
	}

	t, _, err := getPgCodeAndEncodingPlan(typeMap, column.TypeOid, cast)
	if err != nil {
		return nil, err
	}

	// Trying to cast the value according to the given pgtype
	_, err = t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(val))
	if err != nil {
		return nil, fmt.Errorf("cannot decode start value: %w", err)
	}

	return &ReplaceTransformer{
		Column:   column,
		newValue: val,
	}, nil
}

func (rt *ReplaceTransformer) Transform(val string) (string, error) {
	return rt.newValue, nil
}
