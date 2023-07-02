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

	table := &domains.TableMeta{
		Oid: 123,
	}

	tests := []struct {
		name    string
		column  *domains.ColumnMeta
		params  map[string]interface{}
		pattern string
	}{
		{
			name: "test bool type",
			column: &domains.ColumnMeta{
				TypeOid: pgtype.BoolOID,
			},
			params:  map[string]interface{}{},
			pattern: `^(t|f)$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := RandomBoolTransformerMeta.InstanceTransformer(table, tt.column, typeMap, tt.params)
			require.NoError(t, err)
			val, err := transformer.Transform("")
			require.NoError(t, err)
			log.Println(val)
			require.Regexp(t, tt.pattern, val)
		})
	}
}
