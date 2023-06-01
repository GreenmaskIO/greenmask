package transformers

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestUuidTransformer_Transform(t *testing.T) {
	var connStr = "user=postgres dbname=demo"
	c, err := pgx.Connect(context.Background(), connStr)
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
	require.ErrorContains(t, err, "cannot decode value: strconv.ParseInt")
}
