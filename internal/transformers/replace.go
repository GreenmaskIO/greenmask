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

var ReplaceTransformerMeta = TransformerMeta{
	Description: `Replace with value passed through "value" parameter`,
	ParamsDescription: map[string]string{
		"value": "replacing value",
	},
	SupportedTypeOids: []int{
		AnyOid,
	},
	NewTransformer: NewReplaceTransformer,
}

func NewReplaceTransformer(column pgDomains.ColumnMeta, typeMap *pgtype.Map, params map[string]string) (domains.Transformer, error) {
	var cast string
	val, ok := params["value"]
	if !ok {
		return nil, errors.New("expected value key")
	}

	t, _, err := GetPgTypeAndEncodingPlan(typeMap, column.TypeOid, cast)
	if err != nil {
		return nil, err
	}

	// Trying to cast the value according to the given pgtype
	_, err = t.Codec.DecodeValue(typeMap, t.OID, pgx.TextFormatCode, []byte(val))
	if err != nil {
		return nil, fmt.Errorf("cannot decode min value: %w", err)
	}

	return &ReplaceTransformer{
		Column:   column,
		newValue: val,
	}, nil
}

func (rt *ReplaceTransformer) Transform(val string) (string, error) {
	return rt.newValue, nil
}
