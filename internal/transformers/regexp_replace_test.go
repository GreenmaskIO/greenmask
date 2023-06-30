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

func TestRegexpReplaceTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()

	transformer, err := NewRegexpReplaceTransformer(domains.ColumnMeta{
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
	}, typeMap, "", map[string]interface{}{"regexp": `(Hello)\s*world\s*(\!+\?)`, "replace": "$1 Mr NoName $2"})
	require.NoError(t, err)
	res, err := transformer.Transform("Hello world!!!?")
	require.NoError(t, err)
	require.Equal(t, "Hello Mr NoName !!!?", res)

}
