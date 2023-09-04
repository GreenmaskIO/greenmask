package transformers

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name    string
		table   *toclib2.Table
		params  map[string]interface{}
		useType string
		pattern string
	}{
		{
			name: "default fixed string",
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
				"min":    10,
				"max":    10,
				"column": "test",
			},
			pattern: `^\w{10}$`,
		},
		{
			name: "default floated string",
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
				"min":    2,
				"max":    30,
				"column": "test",
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name: "default floated string",
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
				"min":     10,
				"max":     10,
				"symbols": "1234567890",
				"column":  "test",
			},
			pattern: `^\d{10}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomStringTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			require.NoError(t, err)
			tr := transformer.(*RandomStringTransformer)
			val, err := tr.TransformAttr("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
