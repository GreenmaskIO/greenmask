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
		Columns: []*domains.Column{
			&domains.Column{
				Name: "test1",
				ColumnMeta: domains.ColumnMeta{
					TypeOid: pgtype.UUIDOID,
				},
			},
			&domains.Column{
				Name: "test2",
				ColumnMeta: domains.ColumnMeta{
					TypeOid: pgtype.TextOID,
				},
			},
			&domains.Column{
				Name: "test3",
				ColumnMeta: domains.ColumnMeta{
					TypeOid: pgtype.Int8OID,
				},
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
