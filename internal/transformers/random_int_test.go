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
		params  map[string]string
		pattern string
	}{
		{
			name: "int2",
			column: domains.ColumnMeta{
				Type:    "int2",
				TypeOid: pgtype.Int2OID,
			},
			params: map[string]string{
				"min": "-10000",
				"max": "10000",
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "int4",
			column: domains.ColumnMeta{
				Type:    "int4",
				TypeOid: pgtype.Int4OID,
			},
			params: map[string]string{
				"min": "-10000",
				"max": "10000",
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "int8",
			column: domains.ColumnMeta{
				Type:    "int8",
				TypeOid: pgtype.Int8OID,
			},
			params: map[string]string{
				"min": "-10000",
				"max": "10000",
			},
			pattern: `^-*\d+$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomIntTransformer(tt.column, typeMap, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
