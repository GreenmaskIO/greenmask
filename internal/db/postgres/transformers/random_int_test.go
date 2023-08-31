package transformers

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/toclib"
)

// TODO: Cover error cases
func TestRandomIntTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name    string
		table   *toclib2.Table
		params  map[string]interface{}
		pattern string
	}{
		{
			name: "int2",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Int2OID,
					},
				},
			},
			params: map[string]interface{}{
				"min":    -10000,
				"max":    10000,
				"column": "test",
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "int4",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Int4OID,
					},
				},
			},
			params: map[string]interface{}{
				"min":    -10000,
				"max":    10000,
				"column": "test",
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "int8",
			table: &toclib2.Table{
				Oid: 123,
				Columns: []*toclib2.Column{
					{
						Name:    "test",
						TypeOid: pgtype.Int8OID,
					},
				},
			},
			params: map[string]interface{}{
				"min":    -10000,
				"max":    10000,
				"column": "test",
			},
			pattern: `^-*\d+$`,
		},
		{
			name: "text with int8",
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
				// TODO: If you set 0 it falls as it is not provided
				"min":     1,
				"max":     100,
				"useType": "int8",
				"column":  "test",
			},
			pattern: `^\d{1,3}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomIntTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			require.NoError(t, err)
			tr := transformer.(*RandomIntTransformer)
			val, err := tr.TransformAttr("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
