package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/toclib"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	table := &toclib2.Table{
		Oid: 123,
		Columns: []*toclib2.Column{
			{
				Name:    "test",
				TypeOid: pgtype.UUIDOID,
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
