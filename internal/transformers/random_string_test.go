package transformers

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
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
			name: "default fixed string",
			column: domains.ColumnMeta{
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min": 10,
				"max": 10,
			},
			pattern: `^\w{10}$`,
		},
		{
			name: "default floated string",
			column: domains.ColumnMeta{
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
			},
			params: map[string]interface{}{
				"min": 2,
				"max": 30,
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name: "default floated string",
			column: domains.ColumnMeta{
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
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
			transformer, err := NewRandomStringTransformer(tt.column, typeMap, tt.useType, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
