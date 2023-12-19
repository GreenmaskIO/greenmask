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
	"fmt"
	"testing"

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
		ratio         float64
		columnName    string
		originalValue string
		result        result
	}{
		{
			name:          "int2",
			columnName:    "id2",
			ratio:         0.9,
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
		{
			name:          "int4",
			columnName:    "id4",
			ratio:         0.9,
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
		{
			name:          "int8",
			columnName:    "id8",
			ratio:         0.9,
			result:        result{min: 12, max: 234},
			originalValue: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := NoiseIntTransformerDefinition.Instance(
				context.Background(),
				driver, map[string]toolkit.ParamsValue{
					"column": toolkit.ParamsValue(tt.columnName),
					"ratio":  toolkit.ParamsValue(fmt.Sprintf("%f", tt.ratio)),
				},
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res int64
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.Equal(t, tt.result.isNull, isNull)
			if !isNull {
				assert.GreaterOrEqual(t, res, tt.result.min)
				assert.LessOrEqual(t, res, tt.result.max)
			}
		})
	}
}
