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

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRegexpReplaceTransformer_Transform2(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string]toolkit.ParamsValue
		columnName string
		original   string
		expected   string
	}{
		{
			name: "common",
			params: map[string]toolkit.ParamsValue{
				"regexp":  toolkit.ParamsValue(`(Hello)\s*world\s*(\!+\?)`),
				"replace": toolkit.ParamsValue("$1 Mr NoName $2"),
			},
			columnName: "data",
			original:   "Hello world!!!?",
			expected:   "Hello Mr NoName !!!?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformerCtx, warnings, err := RegexpReplaceTransformerDefinition.Instance(
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
			var res string
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, tt.expected, res)
		})
	}

}
