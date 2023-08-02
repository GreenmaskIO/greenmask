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
		Columns: []*domains.Column{
			&domains.Column{
				Name: "test",
				ColumnMeta: domains.ColumnMeta{
					TypeOid: pgtype.UUIDOID,
				},
			},
		},
	}

	transformer, err := SetNullTransformerMeta.InstanceTransformer(
		table,
		typeMap,
		map[string]interface{}{
			"column": "test",
		})
	require.NoError(t, err)
	tr := transformer.(*SetNullTransformer)
	res, err := tr.TransformAttr("old_val")
	assert.Equal(t, `\N`, res)
}
