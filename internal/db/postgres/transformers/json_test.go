package transformers

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	toclib2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/toclib"
)

func TestJsonTransformer_Transform(t *testing.T) {
	typeMap, err := getTypeMap()
	require.NoError(t, err)

	transformer, err := JsonTransformerMeta.InstanceTransformer(
		&toclib2.Table{
			Oid: 123,
			Columns: []*toclib2.Column{
				{
					Name:    "test",
					TypeOid: pgtype.JSONBOID,
				},
			},
		},
		typeMap,
		map[string]interface{}{
			"column": "test",
			"operations": []map[string]interface{}{
				{
					"operation": "set",
					"path":      "name.first",
					"value":     "Sara",
				},
				{
					"operation": "set",
					"path":      "name.last",
					"value":     "Test",
				},
				{
					"operation": "set",
					"path":      "name.age",
					"value":     10,
				},
				{
					"operation": "delete",
					"path":      "name.todelete",
				},
			},
		})
	tr := transformer.(*JsonTransformer)
	require.NoError(t, err)
	res, err := tr.TransformAttr(`{"name":{"last":"Anderson", "age": 5, "todelete": true}}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":{"last":"Test","first":"Sara", "age": 10}}`, res)
}
