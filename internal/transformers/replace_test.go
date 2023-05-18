package transformers

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestReplaceTransformer_Transform(t *testing.T) {
	var connStr = "user=vvoitenko dbname=demo host=/tmp"
	c, err := pgx.Connect(context.Background(), connStr)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()

	transformer, err := NewReplaceTransformer(domains.ColumnMeta{}, typeMap, nil)
	require.ErrorContains(t, err, "expected value key")

	transformer, err = NewReplaceTransformer(domains.ColumnMeta{
		Type:    "text",
		TypeOid: pgtype.TextOID,
	}, typeMap, map[string]string{"value": "new_val"})
	require.NoError(t, err)
	res, err := transformer.Transform("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "new_val")

	transformer, err = NewReplaceTransformer(domains.ColumnMeta{
		Type:    "date",
		TypeOid: pgtype.DateOID,
	}, typeMap, map[string]string{"value": "new_val"})
	require.ErrorContains(t, err, "cannot decode min value")

	transformer, err = NewReplaceTransformer(domains.ColumnMeta{
		Type:    "date",
		TypeOid: pgtype.DateOID,
	}, typeMap, map[string]string{"value": "2023-18-05"})
	require.NoError(t, err)
	res, err = transformer.Transform("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "2023-18-05")

	transformer, err = NewReplaceTransformer(domains.ColumnMeta{
		Type:    "date",
		TypeOid: pgtype.UUIDOID,
	}, typeMap, map[string]string{"value": "dd88a355-5dfa-4556-aaff-fe18302b285c"})
	require.NoError(t, err)
	res, err = transformer.Transform("3df11ba0-d408-42e1-9306-cd468e0669cb")
	require.NoError(t, err)
	require.Equal(t, res, "dd88a355-5dfa-4556-aaff-fe18302b285c")

}
