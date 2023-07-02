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
	}

	transformer, err := RegexpReplaceTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.TextOID,
		},
		typeMap,
		map[string]interface{}{
			"regexp":  `(Hello)\s*world\s*(\!+\?)`,
			"replace": "$1 Mr NoName $2",
		},
	)
	require.NoError(t, err)
	res, err := transformer.Transform("Hello world!!!?")
	require.NoError(t, err)
	require.Equal(t, "Hello Mr NoName !!!?", res)

}
