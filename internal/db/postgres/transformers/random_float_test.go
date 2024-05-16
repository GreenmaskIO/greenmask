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

func TestRandomFloatTransformer_Transform(t *testing.T) {
	type result struct {
		min    float64
		max    float64
		isNull bool
	}

	tests := []struct {
		name          string
		params        map[string]toolkit.ParamsValue
		columnName    string
		originalValue string
		result        result
	}{
		{
			name:          "float4",
			columnName:    "col_float4",
			originalValue: "1000.0",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("10"),
			},
			result: result{
				min: 1,
				max: 10,
			},
		},
		{
			name:          "float8",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("10"),
			},
			result: result{
				min: 1,
				max: 10,
			},
		},
		{
			name:          "float8 ranges 1",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string]toolkit.ParamsValue{
				"min":     toolkit.ParamsValue("-100000"),
				"max":     toolkit.ParamsValue("100000"),
				"decimal": toolkit.ParamsValue("10"),
			},
			result: result{
				min: -100000,
				max: 100000,
			},
		},
		{
			name:          "float8 ranges 1 with decimal",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string]toolkit.ParamsValue{
				"min":     toolkit.ParamsValue("-100000"),
				"max":     toolkit.ParamsValue("-1"),
				"decimal": toolkit.ParamsValue("0"),
			},
			result: result{
				min: -100000,
				max: -1,
			},
		},
		{
			name:          "keep_null false and NULL seq",
			columnName:    "col_float8",
			originalValue: "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("-100000"),
				"max":       toolkit.ParamsValue("-1"),
				"decimal":   toolkit.ParamsValue("0"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			result: result{
				min: -100000,
				max: -1,
			},
		},
		{
			name:          "keep_null true and NULL seq",
			columnName:    "col_float8",
			originalValue: "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("-100000"),
				"max":       toolkit.ParamsValue("-1"),
				"decimal":   toolkit.ParamsValue("0"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			result: result{
				isNull: true,
			},
		},
		//{
		//	name: "text with default float8",
		//	params: map[string]toolkit.ParamsValue{
		//		"min":       toolkit.ParamsValue("-100000"),
		//		"max":       toolkit.ParamsValue("10.1241"),
		//		"decimal": toolkit.ParamsValue("3"),
		//		"useType":   toolkit.ParamsValue("float4"),
		//	},
		//	result: result{
		//		pattern: `^-*\d+[.]*\d{0,3}$`,
		//	},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformerCtx, warnings, err := floatTransformerDefinition.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res float64
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.Equal(t, tt.result.isNull, isNull)
			if !tt.result.isNull {
				assert.GreaterOrEqual(t, res, tt.result.min)
				assert.LessOrEqual(t, res, tt.result.max)
			}
		})
	}
}
