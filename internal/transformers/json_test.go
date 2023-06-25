package transformers

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestJsonTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()

	transformer, err := NewJsonTransformer(domains.ColumnMeta{
		Type:    "text",
		TypeOid: pgtype.JSONBOID,
	}, typeMap, "", map[string]interface{}{
		"operations": []map[string]interface{}{
			{
				"operation": "set",
				"path":      "name.first",
				"value":     "Sara",
			},
			{
				"operation": "set",
				"path":      "name.last",
				"value":     "Test",
			},
			{
				"operation": "set",
				"path":      "name.age",
				"value":     10,
			},
			{
				"operation": "delete",
				"path":      "name.todelete",
			},
		},
	})
	require.NoError(t, err)
	res, err := transformer.Transform(`{"name":{"last":"Anderson", "age": 5, "todelete": true}}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":{"last":"Test","first":"Sara", "age": 10}}`, res)
}
