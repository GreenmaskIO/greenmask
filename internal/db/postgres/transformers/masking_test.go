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

func TestMaskingTransformer_Transform(t *testing.T) {

	tests := []struct {
		name          string
		originalValue string
		params        map[string]toolkit.ParamsValue
		expectedValue *toolkit.Value
	}{
		{
			name: MMobile,
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MMobile),
			},
			originalValue: "+35798665784",
			expectedValue: toolkit.NewValue("+357***65784", false),
		},
		{
			name: MName,
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MName),
			},
			originalValue: "abcdef test",
			expectedValue: toolkit.NewValue("a**def t**t", false),
		},
		{
			name: MPassword,
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MPassword),
			},
			originalValue: "password_secure",
			expectedValue: toolkit.NewValue("************", false),
		},
		{
			name: MDefault,
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MDefault),
			},
			originalValue: "1234567890",
			expectedValue: toolkit.NewValue("**********", false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			columnName := string(tt.params["column"])
			driver, record := getDriverAndRecord(columnName, tt.originalValue)

			transformer, warnings, err := MaskingTransformerDefinition.Instance(
				context.Background(),
				driver, tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			res, err := r.GetColumnValueByName(columnName)
			require.NoError(t, err)

			require.Equal(t, tt.expectedValue.IsNull, res.IsNull)
			require.Equal(t, tt.expectedValue.Value, res.Value)
		})
	}
}

func TestMaskingTransformer_type_validation(t *testing.T) {
	var columnName = "data"
	var originalValue = "someval"
	driver, _ := getDriverAndRecord(columnName, originalValue)

	_, warnings, err := MaskingTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column": toolkit.ParamsValue(columnName),
			"type":   toolkit.ParamsValue("unknown"),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0].Msg, "unknown type")
}
