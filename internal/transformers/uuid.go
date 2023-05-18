package transformers

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

type UuidTransformer struct {
	Column pgDomains.ColumnMeta
}

func NewUuidTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var cast = "db9abb12-3e84-4873-915d-27c17a1fea22"

	t, _, err := getPgCodeAndEncodingPlan(typeMap, column.TypeOid, cast)
	if err != nil {
		return nil, err
	}

	// Trying to cast the value according to the given pgtype
	if _, err = t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(cast)); err != nil {
		return nil, fmt.Errorf("type %s does not support uuid: %w", t.Name, err)
	}

	return &UuidTransformer{
		Column: column,
	}, nil
}

func (rt *UuidTransformer) Transform(val string) (string, error) {
	return uuid.New().String(), nil
}
