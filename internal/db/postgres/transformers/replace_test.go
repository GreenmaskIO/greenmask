package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
)

func TestReplaceTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &toclib.Table{
		Oid: 123,
		Columns: []*toclib.Column{
			{
				Name:    "test",
				TypeOid: pgtype.TextOID,
			},
		},
	}

	transformer, err := ReplaceTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"column": "test",
		},
	)
	require.ErrorContains(t, err, "validation error")

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"value":  "new_val",
			"column": "test",
		},
	)
	require.NoError(t, err)
	tr := transformer.(*ReplaceTransformer)
	res, err := tr.TransformAttr("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "new_val")

	table = &toclib.Table{
		Oid: 123,
		Columns: []*toclib.Column{
			{
				Name:    "test",
				TypeOid: pgtype.DateOID,
			},
		},
	}

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"value":  "new_val",
			"column": "test",
		},
	)
	require.ErrorContains(t, err, "invalid date format")

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"value":  "2023-18-05",
			"column": "test",
		},
	)
	require.NoError(t, err)
	tr = transformer.(*ReplaceTransformer)
	res, err = tr.TransformAttr("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "2023-18-05")

	table = &toclib.Table{
		Oid: 123,
		Columns: []*toclib.Column{
			{
				Name:    "test",
				TypeOid: pgtype.UUIDOID,
			},
		},
	}

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"value":  "dd88a355-5dfa-4556-aaff-fe18302b285c",
			"column": "test",
		},
	)
	require.NoError(t, err)
	tr = transformer.(*ReplaceTransformer)
	res, err = tr.TransformAttr("3df11ba0-d408-42e1-9306-cd468e0669cb")
	require.NoError(t, err)
	require.Equal(t, res, "dd88a355-5dfa-4556-aaff-fe18302b285c")

}
