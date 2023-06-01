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

func TestRandomFloatTransformer_Transform(t *testing.T) {
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
		pattern string
		useType string
	}{
		{
			name: "float4",
			column: domains.ColumnMeta{
				Type:    "float4",
				TypeOid: pgtype.Float4OID,
			},
			params: map[string]interface{}{
				"min": 1,
				"max": 10,
			},
			pattern: `-*\d+[.]*\d*$`,
		},
		{
			name: "float8",
			column: domains.ColumnMeta{
				Type:    "float8",
				TypeOid: pgtype.Float8OID,
			},
			params: map[string]interface{}{
				"min": 1,
				"max": 10,
			},
			pattern: `-*\d+[.]*\d*$`,
		},
		{
			name: "float8 ranges 1",
			column: domains.ColumnMeta{
				Type:    "float8",
				TypeOid: pgtype.Float8OID,
			},
			params: map[string]interface{}{
				"min":       -100000,
				"max":       100000,
				"precision": 10,
			},
			pattern: `^-*\d+[.]*\d{0,10}$`,
		},
		{
			name: "float8 ranges 1 with precision",
			column: domains.ColumnMeta{
				Type:    "float8",
				TypeOid: pgtype.Float8OID,
			},
			params: map[string]interface{}{
				"min":       -100000,
				"max":       -1,
				"precision": 0,
			},
			pattern: `^-\d+$`,
		},
		{
			name: "text with default float8",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min":       -100000,
				"max":       10.1241,
				"precision": 3,
			},
			useType: "float4",
			pattern: `^-*\d+[.]*\d{0,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomFloatTransformer(tt.column, typeMap, tt.useType, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
