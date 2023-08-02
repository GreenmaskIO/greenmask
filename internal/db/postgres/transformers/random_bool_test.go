package transformers

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	tests := []struct {
		name    string
		table   *domains.TableMeta
		params  map[string]interface{}
		pattern string
	}{
		{
			name: "test bool type",
			table: &domains.TableMeta{
				Oid: 123,
				Columns: []*domains.Column{
					&domains.Column{
						Name: "test",
						ColumnMeta: domains.ColumnMeta{
							TypeOid: pgtype.BoolOID,
						},
					},
				},
			},
			params: map[string]interface{}{
				"column": "test",
			},
			pattern: `^(t|f)$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomBoolTransformerMeta.InstanceTransformer(tt.table, typeMap, tt.params)
			require.NoError(t, err)
			tr := transformer.(*RandomBoolTransformer)
			val, err := tr.TransformAttr("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
