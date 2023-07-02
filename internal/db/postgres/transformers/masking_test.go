package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestMaskingTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	transformer, err := MaskingTransformerMeta.InstanceTransformer(
		&domains.TableMeta{
			Oid: 123,
		},
		&domains.ColumnMeta{
			TypeOid: pgtype.TextOID,
		}, typeMap,
		map[string]interface{}{"type": "name"},
	)
	require.NoError(t, err)
	res, err := transformer.Transform("abcdef test")
	require.NoError(t, err)
	require.Equal(t, "a**def t**t", res)

	transformer, err = MaskingTransformerMeta.InstanceTransformer(
		&domains.TableMeta{
			Oid: 123,
		},
		&domains.ColumnMeta{
			TypeOid: pgtype.TextOID,
		},
		typeMap,
		map[string]interface{}{"type": "password"},
	)
	require.NoError(t, err)
	res, err = transformer.Transform("password_secure")
	require.NoError(t, err)
	require.Equal(t, "************", res)

}
