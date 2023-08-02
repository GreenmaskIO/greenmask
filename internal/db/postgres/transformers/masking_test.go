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
