package transformers

import (
	"context"
	"log"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

// TODO: Cover error cases
func TestRandomIntTransformer_Transform(t *testing.T) {
	var connStr = "user=vvoitenko dbname=demo host=/tmp"
	c, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()
	// Positive cases
	tests := []struct {
		name    string
		column  domains.ColumnMeta
		params  map[string]interface{}
		useType string
		pattern string
	}{
		{
			name: "int2",
			column: domains.ColumnMeta{
				Type:    "int2",
				TypeOid: pgtype.Int2OID,
			},
			params: map[string]interface{}{
				"min": -10000,
				"max": 10000,
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "int4",
			column: domains.ColumnMeta{
				Type:    "int4",
				TypeOid: pgtype.Int4OID,
			},
			params: map[string]interface{}{
				"min": -10000,
				"max": 10000,
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "int8",
			column: domains.ColumnMeta{
				Type:    "int8",
				TypeOid: pgtype.Int8OID,
			},
			params: map[string]interface{}{
				"min": -10000,
				"max": 10000,
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "text with int8",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				// TODO: If you set 0 it falls as it is not provided
				"min": 1,
				"max": 100,
			},
			useType: "int8",
			pattern: `^\d{1,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomIntTransformerV2(tt.column, typeMap, tt.useType, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
