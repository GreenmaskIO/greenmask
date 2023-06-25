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

func TestHashTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()

	transformer, err := NewHashTransformer(domains.ColumnMeta{
		Type:    "text",
		TypeOid: pgtype.TextOID,
	}, typeMap, "", map[string]interface{}{
		"salt": "12345678",
	})
	require.NoError(t, err)
	res, err := transformer.Transform("old_value")
	require.NoError(t, err)
	require.Equal(t, res, "9n+v7qGp0ua+DgXtC9ClyjPHjWvWin6fKAmX5bZjcX4=")

}
