package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &domains.TableMeta{
		Oid: 123,
	}

	transformer, err := SetNullTransformerMeta.InstanceTransformer(
		table,
		&domains.ColumnMeta{
			TypeOid: pgtype.UUIDOID,
		},
		typeMap,
		nil)
	require.NoError(t, err)
	res, err := transformer.Transform("old_val")
	assert.Equal(t, `\N`, res)
}
