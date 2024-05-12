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

func TestUuidTransformer_Transform_uuid_type(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		params     map[string]toolkit.ParamsValue
		original   string
		regexp     string
	}{
		{
			name:       "text",
			columnName: "data",
			params:     map[string]toolkit.ParamsValue{},
			original:   "someval",
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "uuid",
			columnName: "uid",
			original:   "ddfb6f74-1771-45b0-b258-ae6fcd42f505",
			params:     map[string]toolkit.ParamsValue{},
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull false and NULL seq",
			columnName: "uid",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"keep_null": toolkit.ParamsValue("false"),
			},
			regexp: `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull true and NULL seq",
			columnName: "uid",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"keep_null": toolkit.ParamsValue("true"),
			},
			regexp: fmt.Sprintf(`^(\%s)$`, "\\N"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformerCtx, warnings, err := uuidTransformerDefinition.Instance(
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
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			assert.Regexp(t, tt.regexp, string(res))
		})
	}
}
