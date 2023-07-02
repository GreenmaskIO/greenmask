package transformers

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &domains.TableMeta{
		Oid: 123,
	}

	tests := []struct {
		name    string
		column  *domains.ColumnMeta
		params  map[string]interface{}
		useType string
		pattern string
	}{
		{
			name: "default fixed string",
			column: &domains.ColumnMeta{
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min": 10,
				"max": 10,
			},
			pattern: `^\w{10}$`,
		},
		{
			name: "default floated string",
			column: &domains.ColumnMeta{
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min": 2,
				"max": 30,
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name: "default floated string",
			column: &domains.ColumnMeta{
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min":     10,
				"max":     10,
				"symbols": "1234567890",
			},
			pattern: `^\d{10}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomStringTransformerMeta.InstanceTransformer(table, tt.column, typeMap, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
