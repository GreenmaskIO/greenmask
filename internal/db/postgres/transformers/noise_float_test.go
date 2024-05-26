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

	"github.com/rs/zerolog/log"
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
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `-*\d+[.]*\d*$`},
		},
		{
			name:       "float8",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `-*\d+[.]*\d*$`},
		},
		{
			name:       "float8 ranges 1",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
				"decimal":   toolkit.ParamsValue("10"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+[.]*\d{0,10}$`},
		},
		{
			name:       "float8 ranges 1 with decimal",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
				"decimal":   toolkit.ParamsValue("0"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+$`},
		},
		{
			name:       "float8 ranges 1 with decimal and hash engine",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
				"decimal":   toolkit.ParamsValue("0"),
				"engine":    toolkit.ParamsValue("hash"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+$`},
		},
		{
			name:       "with thresholds",
			columnName: "col_float8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
				"min":       toolkit.ParamsValue("90"),
				"max":       toolkit.ParamsValue("110"),
				"decimal":   toolkit.ParamsValue("0"),
			},
			input:  "100",
			result: result{min: 90, max: 110, regexp: `^-*\d+$`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.input)
			transformerCtx, warnings, err := NoiseFloatTransformerDefinition.Instance(
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
			assert.False(t, isNull)

			log.Debug().Str("Original", tt.input).Float64("Transformed", res).Msg("")
			if !isNull {
				assert.GreaterOrEqual(t, res, tt.result.min)
				assert.LessOrEqual(t, res, tt.result.max)
				rawValue, err := r.GetRawColumnValueByName(tt.columnName)
				require.NoError(t, err)
				require.NoError(t, err)
				require.False(t, rawValue.IsNull)
				require.Regexp(t, tt.result.regexp, string(rawValue.Data))
				require.NoError(t, err)
			}
		})
	}
}
