package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"
)

func TestMaskingTransformer_Transform(t *testing.T) {
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

	transformer, err := MaskingTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"type":   "name",
			"column": "test",
		},
	)
	require.NoError(t, err)
	tr := transformer.(*MaskingTransformer)
	res, err := tr.TransformAttr("abcdef test")
	require.NoError(t, err)
	require.Equal(t, "a**def t**t", res)

	transformer, err = MaskingTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"type":   "password",
			"column": "test",
		},
	)
	require.NoError(t, err)
	tr = transformer.(*MaskingTransformer)
	res, err = tr.TransformAttr("password_secure")
	require.NoError(t, err)
	require.Equal(t, "************", res)

}
