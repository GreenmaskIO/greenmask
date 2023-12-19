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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestNoiseFloatTransformer_Transform(t *testing.T) {

	type result struct {
		min    float64
		max    float64
		regexp string
	}

	tests := []struct {
		name       string
		columnName string
		params     map[string]toolkit.ParamsValue
		input      string
		result     result
	}{
		{
			name:       "float4",
			columnName: "col_float4",
			params: map[string]toolkit.ParamsValue{
				"ratio": toolkit.ParamsValue("0.9"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `-*\d+[.]*\d*$`},
		},
		{
			name:       "float8",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"ratio": toolkit.ParamsValue("0.9"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `-*\d+[.]*\d*$`},
		},
		{
			name:       "float8 ranges 1",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"ratio":     toolkit.ParamsValue("0.9"),
				"precision": toolkit.ParamsValue("10"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+[.]*\d{0,10}$`},
		},
		{
			name:       "float8 ranges 1 with precision",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"ratio":     toolkit.ParamsValue("0.9"),
				"precision": toolkit.ParamsValue("0"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+$`},
		},
		//{
		//	name: "text with default float8",
		//	params: map[string]toolkit.ParamsValue{
		//		"ratio":     0.9,
		//		"precision": 3,
		//		"useType":   "float4",
		//		"column":    "test",
		//	},
		//	input:   "100",
		//	result:  result{min: 10, max: 190},
		//	regexp: `^-*\d+[.]*\d{0,3}$`,
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.input)
			transformer, warnings, err := NoiseFloatTransformerDefinition.Instance(
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
			var res float64
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			assert.False(t, isNull)
			if !isNull {
				assert.GreaterOrEqual(t, res, tt.result.min)
				assert.LessOrEqual(t, res, tt.result.max)
				encodedValue, err := r.Encode()
				require.NoError(t, err)
				idx := slices.IndexFunc(driver.Table.Columns, func(column *toolkit.Column) bool {
					return column.Name == tt.columnName
				})
				require.NotEqual(t, -1, idx)
				rawValue, err := encodedValue.GetColumn(idx)
				require.NoError(t, err)
				require.False(t, rawValue.IsNull)
				require.Regexp(t, tt.result.regexp, string(rawValue.Data))
				require.NoError(t, err)
			}
		})
	}
}
