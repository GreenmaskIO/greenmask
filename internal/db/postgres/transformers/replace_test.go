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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

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
			transformerCtx, warnings, err := ReplaceTransformerDefinition.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			attVal, err := r.GetColumnValueByName(tt.columnName)
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

func TestReplaceTransformer_Transform_with_raw_value(t *testing.T) {
	type result struct {
		isNull bool
		value  string
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
			original:   `{}`,
			columnName: "doc",
			params: map[string]toolkit.ParamsValue{
				"value": toolkit.ParamsValue(`{"test": 1234}`),
			},
			result: result{
				isNull: false,
				value:  `{"test": 1234}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			transformerCtx, warnings, err := ReplaceTransformerDefinition.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			attVal, err := r.GetColumnValueByName(tt.columnName)
			require.Equal(t, tt.result.isNull, attVal.IsNull)
			require.NoError(t, err)
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			require.JSONEq(t, tt.result.value, string(res))
		})

	}
}

func TestReplaceTransformer_Transform_dynamic(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		type expected struct {
			isNull        bool
			value         string
			errorContains string
		}

		tests := []struct {
			name          string
			columnName    string
			params        map[string]toolkit.ParamsValue
			dynamicParams map[string]*toolkit.DynamicParamValue
			record        map[string]*toolkit.RawValue
			expected      expected
		}{
			{
				name:       "set int value from another column",
				columnName: "id4",
				record: map[string]*toolkit.RawValue{
					"id4":      toolkit.NewRawValue([]byte("123"), false),
					"int4_val": toolkit.NewRawValue([]byte("10"), false),
				},
				params: map[string]toolkit.ParamsValue{},
				dynamicParams: map[string]*toolkit.DynamicParamValue{
					"value": {
						Column: "int4_val",
					},
				},
				expected: expected{
					isNull: false,
					value:  "10",
				},
			},
			{
				name:       "set null value from another column",
				columnName: "id4",
				record: map[string]*toolkit.RawValue{
					"id4":      toolkit.NewRawValue([]byte("123"), false),
					"int4_val": toolkit.NewRawValue(nil, true),
				},
				params: map[string]toolkit.ParamsValue{},
				dynamicParams: map[string]*toolkit.DynamicParamValue{
					"value": {
						Column: "int4_val",
					},
				},
				expected: expected{
					isNull: true,
					value:  "",
				},
			},
			{
				name:       "different types and validate true",
				columnName: "id4",
				record: map[string]*toolkit.RawValue{
					"id4":  toolkit.NewRawValue([]byte("123"), false),
					"data": toolkit.NewRawValue([]byte("asad"), false),
				},
				params: map[string]toolkit.ParamsValue{},
				dynamicParams: map[string]*toolkit.DynamicParamValue{
					"value": {
						Column: "data",
					},
				},
				expected: expected{
					errorContains: "dynamic value validation error",
				},
			},
			{
				name:       "different types and validate false",
				columnName: "id4",
				record: map[string]*toolkit.RawValue{
					"id4":  toolkit.NewRawValue([]byte("123"), false),
					"data": toolkit.NewRawValue([]byte("asad"), false),
				},
				params: map[string]toolkit.ParamsValue{
					"validate": toolkit.ParamsValue("false"),
				},
				dynamicParams: map[string]*toolkit.DynamicParamValue{
					"value": {
						Column: "data",
					},
				},
				expected: expected{
					isNull: false,
					value:  "asad",
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {

				driver, record := toolkit.GetDriverAndRecord(tt.record)

				tt.params["column"] = toolkit.ParamsValue(tt.columnName)
				def, ok := utils.DefaultTransformerRegistry.Get("Replace")
				require.True(t, ok)

				ctx := context.Background()
				transformer, warnings, err := def.Instance(
					ctx,
					driver,
					tt.params,
					tt.dynamicParams,
					"",
				)
				require.NoError(t, err)
				require.Empty(t, warnings)

				err = transformer.Transformer.Init(ctx)
				require.NoError(t, err)

				for _, dp := range transformer.DynamicParameters {
					dp.SetRecord(record)
				}

				r, err := transformer.Transformer.Transform(
					ctx,
					record,
				)

				if tt.expected.errorContains == "" {
					actual, err := r.GetRawColumnValueByName(tt.columnName)
					require.NoError(t, err)
					require.Equal(t, tt.expected.isNull, actual.IsNull)
					require.Equal(t, tt.expected.value, string(actual.Data))
				} else {
					require.ErrorContains(t, err, tt.expected.errorContains)
				}
			})
		}
	})
}

func TestReplaceTransformer_Transform_with_validation_error(t *testing.T) {

	original := "doc"
	columnName := "doc"
	params := map[string]toolkit.ParamsValue{
		"column":   toolkit.ParamsValue(columnName),
		"value":    toolkit.ParamsValue(`{"test": 1a234}`),
		"validate": toolkit.ParamsValue("true"),
	}
	driver, _ := getDriverAndRecord(columnName, original)

	_, warnings, err := ReplaceTransformerDefinition.Instance(
		context.Background(),
		driver,
		params,
		nil,
		"",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, warnings)
	assert.Equal(t, warnings[0].Severity, toolkit.ErrorValidationSeverity)
}
