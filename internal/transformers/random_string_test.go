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
func TestRandomStringTransformer_Transform(t *testing.T) {
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
			name: "default fixed string",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]string{
				"minLength": "10",
				"maxLength": "10",
			},
			pattern: `^\w{10}$`,
		},
		{
			name: "default floated string",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]string{
				"minLength": "2",
				"maxLength": "30",
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name: "default floated string",
			column: domains.ColumnMeta{
				Type:    "text",
				TypeOid: pgtype.TextOID,
			},
			params: map[string]string{
				"minLength": "10",
				"maxLength": "10",
				"symbols":   "1234567890",
			},
			pattern: `^\d{10}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewRandomStringTransformer(tt.column, typeMap, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
