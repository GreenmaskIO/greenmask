package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestReplaceTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &domains.TableMeta{
		Oid: 123,
	}

	transformer, err := ReplaceTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.TextOID,
		},
		typeMap,
		nil,
	)
	require.ErrorContains(t, err, "validation error")

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.TextOID,
		},
		typeMap,
		map[string]interface{}{
			"value": "new_val",
		},
	)
	require.NoError(t, err)
	res, err := transformer.Transform("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "new_val")

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.DateOID,
		},
		typeMap,
		map[string]interface{}{
			"value": "new_val",
		},
	)
	require.ErrorContains(t, err, "invalid date format")

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.DateOID,
		},
		typeMap,
		map[string]interface{}{
			"value": "2023-18-05",
		},
	)
	require.NoError(t, err)
	res, err = transformer.Transform("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "2023-18-05")

	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.UUIDOID,
		},
		typeMap,
		map[string]interface{}{
			"value": "dd88a355-5dfa-4556-aaff-fe18302b285c",
		},
	)
	require.NoError(t, err)
	res, err = transformer.Transform("3df11ba0-d408-42e1-9306-cd468e0669cb")
	require.NoError(t, err)
	require.Equal(t, res, "dd88a355-5dfa-4556-aaff-fe18302b285c")

}
