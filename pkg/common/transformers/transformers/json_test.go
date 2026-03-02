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
	"time"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestJsonTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		columnName       string
		staticParameters map[string]models.ParamsValue
		dynamicParameter map[string]models.DynamicParamValue
		original         *models.ColumnRawValue
		expected         *models.ColumnRawValue
		validateFn       func(t *testing.T, expected, actual *models.ColumnRawValue)
		expectedErr      string
		columns          []models.Column
	}{
		{
			name: "simple set and delete",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`
					[
						{"operation": "set", "path": "name.first", "value": "Sara"},
						{"operation": "set", "path": "name.last", "value": "Test"},
						{"operation": "set", "path": "name.age", "value": 10},
						{"operation": "delete", "path": "name.todelete"}
					]
				`),
			},
			original: models.NewColumnRawValue([]byte("123"), false),
			expected: models.NewColumnRawValue(
				[]byte(`{"name":{"last":"Test","first":"Sara", "age": 10}}`),
				false,
			),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				assert.Equal(t, expected.IsNull, actual.IsNull)
				assert.JSONEq(t, string(expected.Data), string(actual.Data))
			},
		},
		{
			name: "with template",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`[
				{
					"operation": "set", 
					"path": "name.ts", 
					"value_template": "{{- .GetOriginalValue | .DecodeValueByType \"timestamp\" | noiseDatePgInterval \"1 year 6 mon 1 day\" | .EncodeValueByType \"timestamp\" | toJsonRawValue -}}"
				}
			]`),
			},
			original: models.NewColumnRawValue(
				[]byte(`{"name":{"ts": "2023-11-23 19:54:49.277332"}}`),
				false,
			),
			expected: models.NewColumnRawValue(
				nil,
				false,
			),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				minValue := time.UnixMilli(1653249289277332000 / int64(time.Millisecond))
				maxValue := time.UnixMilli(1748116489277332000 / int64(time.Millisecond))
				assert.Equal(t, expected.IsNull, actual.IsNull)
				resStr := gjson.GetBytes(actual.Data, "name.ts").Str
				tableDriver := dbmsdriver.New()
				resAny, err := tableDriver.DecodeValueByTypeName(dbmsdriver.TypeTimestamp, []byte(resStr))
				require.NoError(t, err)
				resTime := resAny.(time.Time)
				assert.WithinRange(t, resTime, minValue, maxValue)
			},
		},
		{
			name: "null value",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`
					[
						{"operation": "set", "path": "name.first", "value": "Sara"},
						{"operation": "set", "path": "name.last", "value": "Test"},
						{"operation": "set", "path": "name.age", "value": 10},
						{"operation": "delete", "path": "name.todelete"}
					]
				`),
			},
			original: models.NewColumnRawValue(nil, true),
			expected: models.NewColumnRawValue(
				nil,
				true,
			),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				assert.Equal(t, expected.IsNull, actual.IsNull)
			},
		},
		{
			name: "invalid json and skip_invalid_json false",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`
					[
						{"operation": "set", "path": "name.first", "value": "Sara"},
						{"operation": "set", "path": "name.last", "value": "Test"},
						{"operation": "set", "path": "name.age", "value": 10},
						{"operation": "delete", "path": "name.todelete"}
					]
				`),
			},
			original:   models.NewColumnRawValue([]byte(`{`), false),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			expectedErr: errInvalidJson.Error(),
		},
		{
			name: "invalid json and skip_invalid_json true",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`
					[
						{"operation": "set", "path": "name.first", "value": "Sara"},
						{"operation": "set", "path": "name.last", "value": "Test"},
						{"operation": "set", "path": "name.age", "value": 10},
						{"operation": "delete", "path": "name.todelete"}
					]
				`),
				"skip_invalid_json": models.ParamsValue("true"),
			},
			original:   models.NewColumnRawValue([]byte(`{`), false),
			expected:   models.NewColumnRawValue([]byte(`{`), false),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				assert.Equal(t, expected.IsNull, actual.IsNull)
				assert.Equal(t, string(expected.Data), string(actual.Data))
			},
		},
		{
			name: "skip if key does exits and key does not exist",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`
					[
						{"operation": "set", "path": "key1.unknown", "value": "modified", "skip_not_exist": true}
					]
				`),
			},
			original:   models.NewColumnRawValue([]byte(`{"key1": {"key2": "value"}}`), false),
			expected:   models.NewColumnRawValue([]byte(`{"key1": {"key2": "value"}}`), false),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				assert.Equal(t, expected.IsNull, actual.IsNull)
				assert.Equal(t, string(expected.Data), string(actual.Data))
			},
		},
		{
			name: "skip if key does exits and key exists",
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"operations": models.ParamsValue(`
					[
						{"operation": "set", "path": "key1.key2", "value": "modified", "skip_not_exist": true}
					]
				`),
			},
			original:   models.NewColumnRawValue([]byte(`{"key1": {"key2": "value"}}`), false),
			expected:   models.NewColumnRawValue([]byte(`{"key1": {"key2": "modified"}}`), false),
			columnName: "data",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  dbmsdriver.TypeText,
					TypeClass: models.TypeClassText,
					TypeOID:   dbmsdriver.VirtualOidText,
				},
			},
			validateFn: func(t *testing.T, expected, actual *models.ColumnRawValue) {
				assert.Equal(t, expected.IsNull, actual.IsNull)
				assert.Equal(t, string(expected.Data), string(actual.Data))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				JsonTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original)

			err = env.Transform(t, ctx)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			rec := env.GetRecord()
			actual, err := rec.GetRawColumnValueByName(tt.columnName)
			require.NoError(t, err)
			tt.validateFn(t, tt.expected, actual)
		})
	}
}
