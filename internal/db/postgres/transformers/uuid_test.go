package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestUuidTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &domains.TableMeta{
		Oid: 123,
	}

	transformer, err := UuidTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.UUIDOID,
		},
		typeMap,
		nil,
	)
	require.NoError(t, err)
	res, err := transformer.Transform("old_val")
	assert.NoError(t, err)
	assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res)

	transformer, err = UuidTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.TextOID,
		},
		typeMap,
		nil,
	)
	require.NoError(t, err)
	res, err = transformer.Transform("old_val")
	assert.NoError(t, err)
	assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res)

	transformer, err = UuidTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.Int8OID,
		},
		typeMap,
		nil,
	)
	require.ErrorContains(t, err, "type is not supported")
}
