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

func TestRandomIntTransformer_Transform(t *testing.T) {

	tests := []struct {
		name           string
		columnName     string
		originalValue  string
		expectedRegexp string
		params         map[string]toolkit.ParamsValue
	}{
		{
			name:           "int2",
			columnName:     "id2",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("100"),
			},
		},
		{
			name:           "int4",
			columnName:     "id4",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("100"),
			},
		},
		{
			name:           "int8",
			columnName:     "id8",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("100"),
			},
		},
		{
			name:           "keep_null false and NULL seq",
			columnName:     "id8",
			originalValue:  "\\N",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1"),
				"max":       toolkit.ParamsValue("100"),
				"keep_null": toolkit.ParamsValue("false"),
			},
		},
		{
			name:           "keep_null true and NULL seq",
			columnName:     "id8",
			originalValue:  "\\N",
			expectedRegexp: fmt.Sprintf(`^(\%s)$`, "\\N"),
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1"),
				"max":       toolkit.ParamsValue("100"),
				"keep_null": toolkit.ParamsValue("true"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := RandomIntTransformerDefinition.Instance(
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

			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			require.Regexp(t, tt.expectedRegexp, string(res))
		})
	}

}
