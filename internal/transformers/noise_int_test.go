package transformers

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

// TODO: Test the max/min value exceeded
func TestNoiseIntTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()
	// Positive cases
	tests := []struct {
		name   string
		column domains.ColumnMeta
		params map[string]interface{}
		input  string
		result struct {
			min, max int64
		}
		useType string
		pattern string
	}{
		{
			name: "int2",
			column: domains.ColumnMeta{
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
			},
			params: map[string]interface{}{
				"ratio": 0.9,
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "int4",
			column: domains.ColumnMeta{
				TypeName: "int4",
				TypeOid:  pgtype.Int4OID,
			},
			params: map[string]interface{}{
				"ratio": 0.9,
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "int8",
			column: domains.ColumnMeta{
				TypeName: "int8",
				TypeOid:  pgtype.Int8OID,
			},
			params: map[string]interface{}{
				"ratio": 0.9,
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "text with int8",
			column: domains.ColumnMeta{
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
			},
			params: map[string]interface{}{
				"ratio": 0.9,
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			useType: "int8",
			pattern: `^\d{1,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewNoiseIntTransformer(tt.column, typeMap, tt.useType, tt.params)
			require.NoError(t, err)
			res, err := transformer.Transform(tt.input)
			require.NoError(t, err)
			log.Println(res)
			require.Regexp(t, tt.pattern, res)
			resInt, err := strconv.ParseInt(res, 10, 64)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, resInt, tt.result.min)
			assert.LessOrEqual(t, resInt, tt.result.max)
		})
	}
}
