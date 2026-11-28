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
		{
			name: MPostcode,
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MPostcode),
			},
			originalValue: "SW1A 1AA",
			expectedValue: toolkit.NewValue("SW******", false),
		},
		{
			name: MDefault + " multibyte",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MDefault),
			},
			originalValue: "Иван",
			expectedValue: toolkit.NewValue("****", false),
		},
		{
			name: MPostcode + " multibyte",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MPostcode),
			},
			originalValue: "Ц1Б 2Д",
			expectedValue: toolkit.NewValue("Ц1****", false),
		},
		{
			name: MTelephone + " 10 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MTelephone),
			},
			originalValue: "(02) 2799-3078",
			expectedValue: toolkit.NewValue("(02)2799-****", false),
		},
		{
			name: MTelephone + " 8 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MTelephone),
			},
			originalValue: "2799-3078",
			expectedValue: toolkit.NewValue("2799-****", false),
		},
		{
			name: MTelephone + " international",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MTelephone),
			},
			originalValue: "+1 (415) 555-2671",
			expectedValue: toolkit.NewValue("+1415555****", false),
		},
		{
			name: MTelephone + " 7 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MTelephone),
			},
			originalValue: "555-0187",
			expectedValue: toolkit.NewValue("555****", false),
		},
		{
			name: MTelephone + " shorter than 4 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MTelephone),
			},
			originalValue: "112",
			expectedValue: toolkit.NewValue("***", false),
		},
		{
			name: MEmail,
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MEmail),
			},
			originalValue: "ggw.chang@gmail.com",
			expectedValue: toolkit.NewValue("ggw****@gmail.com", false),
		},
		{
			name: MEmail + " long local part",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MEmail),
			},
			originalValue: "alexandra.johnson@example.com",
			expectedValue: toolkit.NewValue("ale****@example.com", false),
		},
		{
			name: MEmail + " without domain",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MEmail),
			},
			originalValue: "alexandra.johnson",
			expectedValue: toolkit.NewValue("ale****", false),
		},
		{
			name: MEmail + " short local part",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MEmail),
			},
			originalValue: "bob@example.com",
			expectedValue: toolkit.NewValue("bob****@example.com", false),
		},
		{
			name: MEmail + " multibyte",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MEmail),
			},
			originalValue: "иван.петров@example.com",
			expectedValue: toolkit.NewValue("ива****@example.com", false),
		},
		{
			name: MID + " 10 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MID),
			},
			originalValue: "A123456789",
			expectedValue: toolkit.NewValue("A12345****", false),
		},
		{
			name: MID + " 9 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MID),
			},
			originalValue: "295847284",
			expectedValue: toolkit.NewValue("29584****", false),
		},
		{
			name: MID + " 5 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MID),
			},
			originalValue: "12345",
			expectedValue: toolkit.NewValue("1****", false),
		},
		{
			name: MID + " 13 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MID),
			},
			originalValue: "1234567890123",
			expectedValue: toolkit.NewValue("123456789****", false),
		},
		{
			name: MID + " shorter than 4 characters",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MID),
			},
			originalValue: "123",
			expectedValue: toolkit.NewValue("***", false),
		},
		{
			name: MTelephone + " multibyte",
			params: map[string]toolkit.ParamsValue{
				"column": toolkit.ParamsValue("data"),
				"type":   toolkit.ParamsValue(MTelephone),
			},
			originalValue: "０２２７９９３０７８",
			expectedValue: toolkit.NewValue("(０２)２７９９-****", false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			columnName := string(tt.params["column"])
			driver, record := getDriverAndRecord(columnName, tt.originalValue)

			transformerCtx, warnings, err := MaskingTransformerDefinition.Instance(
				context.Background(),
				driver, tt.params,
				nil,
				"",
				false,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
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
		"",
		false,
	)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0].Msg, "unknown type")
}
