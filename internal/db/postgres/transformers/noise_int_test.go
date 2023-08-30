package transformers

import (
	"log"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
)

// TODO: Test the max/min value exceeded
func TestNoiseIntTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	// Positive cases
	tests := []struct {
		name   string
		params map[string]interface{}
		input  string
		table  *toclib.Table
		result struct {
			min, max int64
		}
		pattern string
	}{
		{
			name: "int2",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Int2OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":  0.9,
				"column": "test",
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "int4",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Int4OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":  0.9,
				"column": "test",
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "int8",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Int8OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":  0.9,
				"column": "test",
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "text with int8",
			table: &toclib.Table{
				Oid: 123,
				Columns: []*toclib.Column{
					{
						Name:    "test",
						TypeOid: pgtype.TextOID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":   0.9,
				"useType": "int8",
				"column":  "test",
			},
			input:   "100",
			result:  struct{ min, max int64 }{min: 10, max: 190},
			pattern: `^\d{1,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NoiseIntTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			require.NoError(t, err)
			tr := transformer.(*NoiseIntTransformer)
			res, err := tr.TransformAttr(tt.input)
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
