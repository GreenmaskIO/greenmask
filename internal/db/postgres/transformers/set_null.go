package transformers

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

var SetNullTransformerSupportedOids = []int{
	AnyOid,
}

var SetNullTransformerMeta = TransformerMeta{
	Description:       `Set NULL value`,
	SupportedTypeOids: SetNullTransformerSupportedOids,
	NewTransformer:    NewSetNullTransformer,
	Settings: NewTransformerSettings().
		SetNullable(),
}

type SetNullTransformer struct {
	TransformerBase
	nullSequence string
}

func NewSetNullTransformer(
	table *pgDomains.TableMeta,
	column *pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	params map[string]interface{},
) (domains.Transformer, error) {
	params["nullable"] = true
	base, err := NewTransformerBase(table, column, UuidTransformerMeta.Settings, params, typeMap, UuidTransformerSupportedOids, uuid.New())
	if err != nil {
		return nil, fmt.Errorf("cannot build transformer base object: %w", err)
	}

	return &SetNullTransformer{
		TransformerBase: *base,
		nullSequence:    DefaultNullSeq,
	}, nil
}

func (rt *SetNullTransformer) Transform(val string) (string, error) {
	return rt.nullSequence, nil
}
