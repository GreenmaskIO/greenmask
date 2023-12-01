// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
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

	type result struct {
		isNull bool
		value  any
	}

	tests := []struct {
		name       string
		params     map[string]toolkit.ParamsValue
		columnName string
		original   string
		result     result
	}{
		{
			name:       "common",
			original:   "1",
			columnName: "id",
			params: map[string]toolkit.ParamsValue{
				"value": toolkit.ParamsValue("123"),
			},
			result: result{
				isNull: false,
				value:  "123",
			},
		},
		{
			name:       "keep_null false and NULL seq",
			original:   "\\N",
			columnName: "id",
			params: map[string]toolkit.ParamsValue{
				"value":     toolkit.ParamsValue("123"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			result: result{
				isNull: false,
				value:  "123",
			},
		},
		{
			name:       "keep_null true and NULL seq",
			original:   "\\N",
			columnName: "id",
			params: map[string]toolkit.ParamsValue{
				"value":     toolkit.ParamsValue("123"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			result: result{
				isNull: true,
				value:  "\\N",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
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

			attVal, err := r.GetAttributeValueByName(tt.columnName)
			require.Equal(t, tt.result.isNull, attVal.IsNull)
			require.NoError(t, err)
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			require.Equal(t, tt.result.value, string(res))
		})

	}
}
