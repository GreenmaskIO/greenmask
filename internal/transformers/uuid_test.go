package transformers

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestUuidTransformer_Transform(t *testing.T) {
	dsn := os.Getenv("GF_TEST_DSN")
	require.NotEmpty(t, dsn, "GF_TEST_DSN env variable must be set")
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	defer c.Close(context.Background())
	typeMap := c.TypeMap()

	transformer, err := NewUuidTransformer(domains.ColumnMeta{
		Type:    "uuid",
		TypeOid: pgtype.UUIDOID,
	}, typeMap, "", nil)
	require.NoError(t, err)
	res, err := transformer.Transform("old_val")
	assert.NoError(t, err)
	assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res)

	transformer, err = NewUuidTransformer(domains.ColumnMeta{
		Type:    "uuid",
		TypeOid: pgtype.TextOID,
	}, typeMap, "", nil)
	require.NoError(t, err)
	res, err = transformer.Transform("old_val")
	assert.NoError(t, err)
	assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res)

	transformer, err = NewUuidTransformer(domains.ColumnMeta{
		Type:    "uuid",
		TypeOid: pgtype.Int8OID,
	}, typeMap, "", nil)
	require.ErrorContains(t, err, "type is not supported")
}
