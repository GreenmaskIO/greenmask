package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestRegexpReplaceTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &domains.TableMeta{
		Oid: 123,
		Columns: []*domains.Column{
			&domains.Column{
				Name: "test",
				ColumnMeta: domains.ColumnMeta{
					TypeOid: pgtype.TextOID,
				},
			},
		},
	}

	transformer, err := RegexpReplaceTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"regexp":  `(Hello)\s*world\s*(\!+\?)`,
			"replace": "$1 Mr NoName $2",
			"column":  "test",
		},
	)
	require.NoError(t, err)
	tr := transformer.(*RegexpReplaceTransformer)
	res, err := tr.TransformAttr("Hello world!!!?")
	require.NoError(t, err)
	require.Equal(t, "Hello Mr NoName !!!?", res)

}
