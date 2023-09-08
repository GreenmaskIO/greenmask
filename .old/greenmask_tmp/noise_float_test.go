package transformers

import (
	"log"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/greenmaskio/greenmask/internal/db/postgres/domains/toclib"
)

func TestNoiseFloatTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name   string
		table  *toclib2.Table
		params map[string]interface{}
		input  string
		result struct {
			min, max float64
		}
		pattern string
	}{
		{
			name: "float4",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Float4OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":  0.9,
				"column": "test",
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `-*\d+[.]*\d*$`,
		},
		{
			name: "float8",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Float8OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":  0.9,
				"column": "test",
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `-*\d+[.]*\d*$`,
		},
		{
			name: "float8 ranges 1",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Float8OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":     0.9,
				"precision": 10,
				"column":    "test",
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `^-*\d+[.]*\d{0,10}$`,
		},
		{
			name: "float8 ranges 1 with precision",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Float8OID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":     0.9,
				"precision": 0,
				"column":    "test",
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `^-*\d+$`,
		},
		{
			name: "text with default float8",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.TextOID,
					},
				},
			},
			params: map[string]interface{}{
				"ratio":     0.9,
				"precision": 3,
				"useType":   "float4",
				"column":    "test",
			},
			input:   "100",
			result:  struct{ min, max float64 }{min: 10, max: 190},
			pattern: `^-*\d+[.]*\d{0,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := NoiseFloatTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			tr := transformer.(*NoiseFloatTransformer)
			require.NoError(t, err)
			res, err := tr.TransformAttr(tt.input)
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
