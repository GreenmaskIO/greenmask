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

func TestNoiseFloatTransformer_Transform(t *testing.T) {
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
			min, max float64
		}
		useType string
		pattern string
	}{
		{
			name: "float4",
			column: domains.ColumnMeta{
				TypeName: "float4",
				TypeOid:  pgtype.Float4OID,
			},
			params: map[string]interface{}{
				"ratio": 0.9,
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `-*\d+[.]*\d*$`,
		},
		{
			name: "float8",
			column: domains.ColumnMeta{
				TypeName: "float8",
				TypeOid:  pgtype.Float8OID,
			},
			params: map[string]interface{}{
				"ratio": 0.9,
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `-*\d+[.]*\d*$`,
		},
		{
			name: "float8 ranges 1",
			column: domains.ColumnMeta{
				TypeName: "float8",
				TypeOid:  pgtype.Float8OID,
			},
			params: map[string]interface{}{
				"ratio":     0.9,
				"precision": 10,
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `^-*\d+[.]*\d{0,10}$`,
		},
		{
			name: "float8 ranges 1 with precision",
			column: domains.ColumnMeta{
				TypeName: "float8",
				TypeOid:  pgtype.Float8OID,
			},
			params: map[string]interface{}{
				"ratio":     0.9,
				"precision": 0,
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "text with default float8",
			column: domains.ColumnMeta{
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
			},
			params: map[string]interface{}{
				"ratio":     0.9,
				"precision": 3,
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `^-*\d+[.]*\d{0,3}$`,
			useType: "float4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NewNoiseFloatTransformer(tt.column, typeMap, tt.useType, tt.params)
			require.NoError(t, err)
			res, err := transformer.Transform(tt.input)
			require.NoError(t, err)
			log.Println(res)
			require.Regexp(t, tt.pattern, res)
			resFloat, err := strconv.ParseFloat(res, 64)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, resFloat, tt.result.min)
			assert.LessOrEqual(t, resFloat, tt.result.max)
		})
	}
}
