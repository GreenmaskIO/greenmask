package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
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

func TestReplaceTransformer_Transform2(t *testing.T) {
	driver := getDriver()

	originalRecord := []string{"1", toolkit.DefaultNullSeq, "+35798665784"}
	expectedRecord := []string{"123", toolkit.DefaultNullSeq, "+357***65784"}
	transformer, warnings, err := ReplaceTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte("id"),
			"value":  []byte("123"),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transform(
		context.Background(),
		toolkit.NewRecord(
			driver,
			originalRecord,
		),
	)
	require.NoError(t, err)
	res, err := r.Encode()
	require.NoError(t, err)

	require.Equal(t, expectedRecord[0], res[0])
}
