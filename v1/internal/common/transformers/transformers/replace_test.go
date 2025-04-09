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
