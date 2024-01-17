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

func TestTemplateRecordTransformer_Transform_date(t *testing.T) {
	var columnName = "date_ts"
	var template = `
	  {{ $val := .GetColumnValue "date_ts" }}
	  {{ if isNull $val }}
		{{ "2023-11-20 01:00:00" | .DecodeValueByColumn "date_ts" | dateModify "24h" | .SetColumnValue "date_ts" }}
	  {{ else }}
		 {{ "2023-11-20 01:00:00" | .DecodeValueByColumn "date_ts" | dateModify "48h" | .SetColumnValue "date_ts" }}
	  {{ end }}
	`

	tests := []struct {
		name     string
		original string
		expected string
	}{
		{
			name:     "fist cond",
			original: "\\N",
			expected: "2023-11-21 01:00:00",
		},
		{
			name:     "second cond",
			original: "2022-11-20 01:00:00",
			expected: "2023-11-22 01:00:00",
		},
	}

	driver, record := getDriverAndRecord(columnName, "\\N")

	transformerCtx, warnings, err := TemplateRecordTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"template": toolkit.ParamsValue(template),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	for _, tt := range tests {
		_, record = getDriverAndRecord(columnName, tt.original)
		t.Run(tt.name, func(t *testing.T) {
			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(res))
		})
	}

}

func TestTemplateRecordTransformer_Transform_json(t *testing.T) {
	var columnName = "doc"
	var template = `
	  {{ $val := .GetRawColumnValue "doc" }}
	  {{ jsonSet "name" "hello" $val | jsonValidate | .SetColumnValue "doc" }}
	`

	tests := []struct {
		name     string
		original string
		expected string
	}{
		{
			name:     "fist cond",
			original: `{"name": "test"}`,
			expected: `{"name": "hello"}`,
		},
	}

	driver, record := getDriverAndRecord(columnName, "\\N")

	transformerCtx, warnings, err := TemplateRecordTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"template": toolkit.ParamsValue(template),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	for _, tt := range tests {
		_, record = getDriverAndRecord(columnName, tt.original)
		t.Run(tt.name, func(t *testing.T) {
			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(res))
		})
	}

}
