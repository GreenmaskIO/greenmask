package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
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

	tests := []struct {
		name       string
		params     map[string][]byte
		columnName string
		original   string
		expected   string
	}{
		{
			name:       "common",
			original:   "1",
			columnName: "id",
			params: map[string][]byte{
				"value": []byte("123"),
			},
			expected: "123",
		},
		{
			name:       "keepNull false and NULL seq",
			original:   transformers.DefaultNullSeq,
			columnName: "id",
			params: map[string][]byte{
				"value":    []byte("123"),
				"keepNull": []byte("false"),
			},
			expected: "123",
		},
		{
			name:       "keepNull true and NULL seq",
			original:   transformers.DefaultNullSeq,
			columnName: "id",
			params: map[string][]byte{
				"value":    []byte("123"),
				"keepNull": []byte("true"),
			},
			expected: transformers.DefaultNullSeq,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			tt.params["column"] = []byte(tt.columnName)
			transformer, warnings, err := ReplaceTransformerDefinition.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			res, err := r.EncodeAttr(tt.columnName)
			require.NoError(t, err)

			require.Equal(t, tt.expected, string(res))
		})

	}
}
