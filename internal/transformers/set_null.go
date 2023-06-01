package transformers

import (
	"fmt"

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
}

type SetNullTransformer struct {
	Column       pgDomains.ColumnMeta
	nullSequence string
}

func NewSetNullTransformer(
	column pgDomains.ColumnMeta,
	typeMap *pgtype.Map,
	useType string,
	params map[string]interface{},
) (domains.Transformer, error) {
	if column.NotNull {
		return nil, fmt.Errorf("cannot aply null transformer at not null column")
	}

	return &SetNullTransformer{
		Column:       column,
		nullSequence: DefaultNullSeq,
	}, nil
}

func (rt *SetNullTransformer) Transform(val string) (string, error) {
	return rt.nullSequence, nil
}
