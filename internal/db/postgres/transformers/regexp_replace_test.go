package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"
)

func TestRegexpReplaceTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &toclib2.Table{
		Oid: 123,
		Columns: []*toclib2.Column{
			{
				Name:    "test",
				TypeOid: pgtype.TextOID,
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
