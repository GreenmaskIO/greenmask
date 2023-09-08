package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

//func TestReplaceTransformer_Transform(t *testing.T) {
//	typeMap, err := getTypeMap()
//	require.NoError(t, err)
//
//	table := &toclib2.Table{
//		Oid: 123,
//		Columns: []*toclib2.Column{
//			{
//				Name:    "test",
//				TypeOid: pgtype.TextOID,
//			},
//		},
//	}
//
//	transformer, err := ReplaceTransformerMeta.InstanceTransformer(
//		table,
//		typeMap,
//		map[string]interface{}{
//			"column": "test",
//		},
//	)
//	require.ErrorContains(t, err, "validation error")
//
//	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
//		table,
//		typeMap,
//		map[string]interface{}{
//			"value":  "new_val",
//			"column": "test",
//		},
//	)
//	require.NoError(t, err)
//	tr := transformer.(*ReplaceTransformer)
//	res, err := tr.TransformAttr("old_value")
//	require.NoError(t, err)
//	require.Equal(t, res, "new_val")
//
//	table = &toclib2.Table{
//		Oid: 123,
//		Columns: []*toclib2.Column{
//			{
//				Name:    "test",
//				TypeOid: pgtype.DateOID,
//			},
//		},
//	}
//
//	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
//		table,
//		typeMap,
//		map[string]interface{}{
//			"value":  "new_val",
//			"column": "test",
//		},
//	)
//	require.ErrorContains(t, err, "invalid date format")
//
//	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
//		table,
//		typeMap,
//		map[string]interface{}{
//			"value":  "2023-18-05",
//			"column": "test",
//		},
//	)
//	require.NoError(t, err)
//	tr = transformer.(*ReplaceTransformer)
//	res, err = tr.TransformAttr("old_value")
//	require.NoError(t, err)
//	require.Equal(t, res, "2023-18-05")
//
//	table = &toclib2.Table{
//		Oid: 123,
//		Columns: []*toclib2.Column{
//			{
//				Name:    "test",
//				TypeOid: pgtype.UUIDOID,
//			},
//		},
//	}
//
//	transformer, err = ReplaceTransformerMeta.InstanceTransformer(
//		table,
//		typeMap,
//		map[string]interface{}{
//			"value":  "dd88a355-5dfa-4556-aaff-fe18302b285c",
//			"column": "test",
//		},
//	)
//	require.NoError(t, err)
//	tr = transformer.(*ReplaceTransformer)
//	res, err = tr.TransformAttr("3df11ba0-d408-42e1-9306-cd468e0669cb")
//	require.NoError(t, err)
//	require.Equal(t, res, "dd88a355-5dfa-4556-aaff-fe18302b285c")
//
//}

func TestReplaceTransformer_Transform(t *testing.T) {
	var columnName = "id"
	var originalValue = "1"
	var expectedValue = "123"
	driver, record := getDriverAndRecord(columnName, originalValue)

	transformer, warnings, err := ReplaceTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte(columnName),
			"value":  []byte("123"),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.EncodeAttr(columnName)
	require.NoError(t, err)

	require.Equal(t, expectedValue, string(res))
}
