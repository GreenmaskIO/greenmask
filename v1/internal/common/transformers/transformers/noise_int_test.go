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

// TODO: Test the max/min value exceeded
func TestNoiseIntTransformer_Transform(t *testing.T) {

	type result struct {
		min    int64
		max    int64
		isNull bool
	}

	// Positive cases
	tests := []struct {
		name          string
		columnName    string
		params        map[string]toolkit.ParamsValue
		originalValue string
		result        result
	}{
		{
			name:       "int2",
			columnName: "id2",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
			},
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
		{
			name:       "int4",
			columnName: "id4",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
			},
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
		{
			name:       "int8",
			columnName: "id8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
			},
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
		{
			name:       "int8",
			columnName: "id8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
			},
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
		{
			name:       "int8",
			columnName: "id8",
			params: map[string]toolkit.ParamsValue{
				"min_ratio": toolkit.ParamsValue("0.2"),
				"max_ratio": toolkit.ParamsValue("0.9"),
				"min":       toolkit.ParamsValue("0"),
				"max":       toolkit.ParamsValue("110"),
			},
			result:        result{min: 0, max: 110},
			originalValue: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformerCtx, warnings, err := NoiseIntTransformerDefinition.Instance(
				context.Background(),
				driver, tt.params,
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

			var res int64
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.Equal(t, tt.result.isNull, isNull)
			if !isNull {
				log.Debug().
					Str("original", tt.originalValue).
					Int64("transformed", res).Msg("")
				assert.GreaterOrEqual(t, res, tt.result.min)
				assert.LessOrEqual(t, res, tt.result.max)
			}
		})
	}
}
