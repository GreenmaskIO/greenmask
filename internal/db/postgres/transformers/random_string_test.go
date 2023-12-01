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

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string]toolkit.ParamsValue
		pattern    string
	}{
		{
			name:       "default fixed string",
			original:   "some",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("10"),
				"max_length": toolkit.ParamsValue("10"),
			},
			pattern: `^\w{10}$`,
		},
		{
			name:       "default variadic string",
			original:   "some",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("2"),
				"max_length": toolkit.ParamsValue("30"),
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name:       "custom variadic string",
			original:   "some",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("10"),
				"max_length": toolkit.ParamsValue("10"),
				"symbols":    toolkit.ParamsValue("1234567890"),
			},
			pattern: `^\d{10}$`,
		},
		{
			name:       "keep_null",
			original:   "\\N",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("10"),
				"max_length": toolkit.ParamsValue("10"),
				"symbols":    toolkit.ParamsValue("1234567890"),
				"keep_null":  toolkit.ParamsValue("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, "\\N"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RandomStringTransformerDefinition.Instance(
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
			require.NoError(t, err)
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			require.Regexp(t, tt.pattern, string(res))
		})
	}
}
