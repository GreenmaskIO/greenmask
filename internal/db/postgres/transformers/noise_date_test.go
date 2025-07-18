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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestNoiseDateTransformer_Transform(t *testing.T) {
	loc := time.Now().Location()

	type result struct {
		pattern  string
		min, max time.Time
	}

	tests := []struct {
		name     string
		params   map[string]toolkit.ParamsValue
		original string
		result   result
	}{
		{
			name: "test date type",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"column":    toolkit.ParamsValue("date_date"),
			},
			original: "2023-06-25",
			result: result{
				pattern: `^\d{4}-\d{2}-\d{2}$`,
				min:     time.Date(2022, 3, 1, 22, 00, 0, 0, loc),
				max:     time.Date(2024, 8, 29, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp without timezone type",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"column":    toolkit.ParamsValue("date_ts"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
				min:     time.Date(2022, 3, 1, 22, 00, 0, 0, loc),
				max:     time.Date(2024, 8, 29, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp with timezone type",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"column":    toolkit.ParamsValue("date_tstz"),
			},
			original: "2023-06-25 00:00:00.0+03",
			result: result{
				pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
				min:     time.Date(2022, 3, 1, 22, 00, 0, 0, loc),
				max:     time.Date(2024, 8, 29, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp type with Truncate till day",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"truncate":  toolkit.ParamsValue("month"),
				"column":    toolkit.ParamsValue("date_ts"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
				min:     time.Date(2022, 3, 1, 22, 00, 0, 0, loc),
				max:     time.Date(2024, 8, 29, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp type with Truncate till day",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"truncate":  toolkit.ParamsValue("month"),
				"column":    toolkit.ParamsValue("date_ts"),
				"min":       toolkit.ParamsValue("2022-06-01 22:00:00"),
				"max":       toolkit.ParamsValue("2024-01-29 01:01:01.001"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
				min:     time.Date(2022, 3, 1, 22, 00, 0, 0, loc),
				max:     time.Date(2024, 8, 29, 1, 1, 1, 1000, loc),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(string(tt.params["column"]), tt.original)
			transformerCtx, warnings, err := NoiseDateTransformerDefinition.Instance(
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

			res, err := r.GetColumnValueByName(string(tt.params["column"]))
			require.NoError(t, err)
			// Checking typed value
			assert.False(t, res.IsNull)
			resTime, ok := res.Value.(time.Time)
			require.True(t, ok)
			assert.WithinRangef(t, resTime, tt.result.min, tt.result.max, "result is out of range")

			// Checking raw value
			rowDriver, err := r.Encode()
			require.NoError(t, err)
			idx := slices.IndexFunc(driver.Table.Columns, func(column *toolkit.Column) bool {
				return column.Name == string(tt.params["column"])
			})
			require.NotEqual(t, idx, -1)
			rawValue, err := rowDriver.GetColumn(idx)
			require.NoError(t, err)
			require.False(t, rawValue.IsNull)
			require.Regexp(t, tt.result.pattern, string(rawValue.Data))

		})
	}
}

func TestNoiseDateTransformer_Transform_dynamic(t *testing.T) {
	type expected struct {
		min time.Time
		max time.Time
	}

	tests := []struct {
		name          string
		columnName    string
		params        map[string]toolkit.ParamsValue
		dynamicParams map[string]*toolkit.DynamicParamValue
		record        map[string]*toolkit.RawValue
		expected      expected
	}{
		{
			name:       "NoiseDate with dynamic min",
			columnName: "hiredate",
			record: map[string]*toolkit.RawValue{
				"hiredate":  toolkit.NewRawValue([]byte("2009-01-01 00:00:00"), false),
				"birthdate": toolkit.NewRawValue([]byte("1996-01-01 00:00:00"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("100 year 2 mons 3 day 04:05:06.07"),
				"truncate":  toolkit.ParamsValue("month"),
				"max":       toolkit.ParamsValue("2020-01-01 00:00:00"),
				"engine":    toolkit.ParamsValue("hash"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "birthdate",
				},
			},
			expected: expected{
				min: time.Date(1996, 1, 1, 0, 0, 0, 0, time.UTC),
				max: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := toolkit.GetDriverAndRecord(tt.record)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			def, ok := utils.DefaultTransformerRegistry.Get("NoiseDate")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				tt.dynamicParams,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			err = transformer.Transformer.Init(context.Background())
			require.NoError(t, err)

			for _, dp := range transformer.DynamicParameters {
				dp.SetRecord(record)
			}

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res time.Time
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res.After(tt.expected.min) && res.Before(tt.expected.max))
		})
	}
}
