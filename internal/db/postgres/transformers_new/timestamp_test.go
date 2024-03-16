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

package transformers_new

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomDateTransformer_Transform(t *testing.T) {

	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string]toolkit.ParamsValue
		pattern    string
		isNull     bool
	}{
		{
			name:       "test date type",
			columnName: "date_date",
			original:   "2007-09-14",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("2017-09-14"),
				"max": toolkit.ParamsValue("2023-09-14"),
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name:       "test timestamp without timezone type",
			columnName: "date_ts",
			original:   "2008-12-15 23:34:17.946707",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("2018-12-15 23:34:17.946707"),
				"max": toolkit.ParamsValue("2023-09-14 00:00:17.946707"),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name:       "test timestamp with timezone type",
			columnName: "date_tstz",
			original:   "2008-12-15 23:34:17.946707+03",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("2018-12-15 00:00:00.946707+03"),
				"max": toolkit.ParamsValue("2023-09-14 00:00:17.946707+03"),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name:       "test timestamp type with Truncate till day",
			columnName: "date_ts",
			original:   "2008-12-15 23:34:17.946707",
			params: map[string]toolkit.ParamsValue{
				"min":      toolkit.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":      toolkit.ParamsValue("2023-09-14 00:00:17.946707"),
				"truncate": toolkit.ParamsValue("month"),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
		{
			name:       "keep_null false and NULL seq",
			columnName: "date_ts",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":       toolkit.ParamsValue("2023-09-14 00:00:17.946707"),
				"truncate":  toolkit.ParamsValue("month"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, "\\N"),
			isNull:  true,
		},
		{
			name:       "keep_null true and NULL seq",
			columnName: "date_ts",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":       toolkit.ParamsValue("2023-09-14 00:00:17.946707"),
				"truncate":  toolkit.ParamsValue("month"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			def, ok := utils.DefaultTransformerRegistry.Get("random.Timestamp")
			require.True(t, ok)

			transformerCtx, warnings, err := def.Instance(
				context.Background(),
				driver, tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			rowDriver, err := r.Encode()
			require.NoError(t, err)
			idx := slices.IndexFunc(driver.Table.Columns, func(column *toolkit.Column) bool {
				return column.Name == tt.columnName
			})
			require.NotEqual(t, idx, -1)
			rawValue, err := rowDriver.GetColumn(idx)
			require.NoError(t, err)
			require.Equal(t, tt.isNull, rawValue.IsNull)
			if !rawValue.IsNull {
				require.Regexp(t, tt.pattern, string(rawValue.Data))
			}
		})
	}
}
