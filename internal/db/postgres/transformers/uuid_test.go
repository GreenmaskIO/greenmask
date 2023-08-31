package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/toclib"
)

func TestUuidTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &toclib2.Table{
		Oid: 123,
		Columns: []*toclib2.Column{
			{
				Name:    "test1",
				TypeOid: pgtype.UUIDOID,
			},
			{
				Name:    "test2",
				TypeOid: pgtype.TextOID,
			},
			{
				Name:    "test3",
				TypeOid: pgtype.Int8OID,
			},
		},
	}

	transformer, err := RandomUuidTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"column": "test1",
		},
	)
	require.NoError(t, err)
	tr := transformer.(*RandomUuidTransformer)
	res, err := tr.TransformAttr("old_val")
	assert.NoError(t, err)
	assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res)

	transformer, err = RandomUuidTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"column": "test2",
		},
	)
	require.NoError(t, err)
	tr = transformer.(*RandomUuidTransformer)
	res, err = tr.TransformAttr("old_val")
	assert.NoError(t, err)
	assert.Regexp(t, `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`, res)

	transformer, err = RandomUuidTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"column": "test3",
		},
	)
	require.ErrorContains(t, err, "type is not supported")
}
