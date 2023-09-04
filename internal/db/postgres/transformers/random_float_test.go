package transformers

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"
)

func TestRandomFloatTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name    string
		table   *toclib2.Table
		params  map[string]interface{}
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
				"min":    1,
				"max":    10,
				"column": "test",
			},
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
				"min":    1,
				"max":    10,
				"column": "test",
			},
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
				"min":       -100000,
				"max":       100000,
				"precision": 10,
				"column":    "test",
			},
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
				"min":       -100000,
				"max":       -1,
				"precision": 0,
				"column":    "test",
			},
			pattern: `^-\d+$`,
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
				"min":       -100000,
				"max":       10.1241,
				"precision": 3,
				"useType":   "float4",
				"column":    "test",
			},
			pattern: `^-*\d+[.]*\d{0,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomFloatTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			require.NoError(t, err)
			tr := transformer.(*RandomFloatTransformer)
			val, err := tr.TransformAttr("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
