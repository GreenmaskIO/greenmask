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

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	commonutils "github.com/greenmaskio/greenmask/internal/utils"
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
				false,
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

func TestNoiseDateTransformer_Transform_static_limits(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]toolkit.ParamsValue
		original string
		expected time.Time
	}{
		{
			name: "min only",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 day"),
				"min":       toolkit.ParamsValue("2024-01-01 00:00:00"),
			},
			original: "2023-06-25 00:00:00",
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "max only",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 day"),
				"max":       toolkit.ParamsValue("2020-01-01 00:00:00"),
			},
			original: "2023-06-25 00:00:00",
			expected: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "min and max below min",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 day"),
				"min":       toolkit.ParamsValue("2024-01-01 00:00:00"),
				"max":       toolkit.ParamsValue("2024-02-01 00:00:00"),
			},
			original: "2023-06-25 00:00:00",
			expected: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "min and max above max",
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 day"),
				"min":       toolkit.ParamsValue("2020-01-01 00:00:00"),
				"max":       toolkit.ParamsValue("2020-02-01 00:00:00"),
			},
			original: "2023-06-25 00:00:00",
			expected: time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue("date_ts")
			driver, record := getDriverAndRecord("date_ts", tt.original)
			transformerCtx, warnings, err := NoiseDateTransformerDefinition.Instance(
				context.Background(),
				driver, tt.params,
				nil,
				"",
				false,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)
			r, err := transformerCtx.Transformer.Transform(context.Background(), record)
			require.NoError(t, err)

			var res time.Time
			empty, err := r.ScanColumnValueByName("date_ts", &res)
			require.NoError(t, err)
			require.False(t, empty)
			assert.True(t, tt.expected.Equal(res), "expected %s got %s", tt.expected, res)
		})
	}
}

func TestNoiseDateTransformer_Transform_interval_time_part(t *testing.T) {
	tests := []struct {
		name     string
		maxRatio string
		ratio    time.Duration
		original time.Time
	}{
		{
			name:     "one second",
			maxRatio: "00:00:01",
			ratio:    time.Second,
			original: time.Date(2023, 6, 25, 12, 0, 0, 0, time.UTC),
		},
		{
			name:     "one minute",
			maxRatio: "00:01:00",
			ratio:    time.Minute,
			original: time.Date(2023, 6, 25, 12, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]toolkit.ParamsValue{
				"column":    toolkit.ParamsValue("date_ts"),
				"max_ratio": toolkit.ParamsValue(tt.maxRatio),
			}
			for i := 0; i < 50; i++ {
				driver, record := getDriverAndRecord("date_ts", tt.original.Format("2006-01-02 15:04:05"))
				transformerCtx, warnings, err := NoiseDateTransformerDefinition.Instance(
					context.Background(),
					driver, params,
					nil,
					"",
					false,
				)
				require.NoError(t, err)
				require.Empty(t, warnings)
				r, err := transformerCtx.Transformer.Transform(context.Background(), record)
				require.NoError(t, err)

				var res time.Time
				_, err = r.ScanColumnValueByName("date_ts", &res)
				require.NoError(t, err)
				assert.WithinRange(t, res, tt.original.Add(-tt.ratio), tt.original.Add(tt.ratio))
			}
		})
	}
}

func TestNoiseDateTransformer_Transform_equal_ratios(t *testing.T) {
	original := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	params := map[string]toolkit.ParamsValue{
		"column":    toolkit.ParamsValue("date_ts"),
		"min_ratio": toolkit.ParamsValue("1 day"),
		"max_ratio": toolkit.ParamsValue("1 day"),
	}
	for i := 0; i < 10; i++ {
		driver, record := getDriverAndRecord("date_ts", original.Format("2006-01-02 15:04:05"))
		transformerCtx, warnings, err := NoiseDateTransformerDefinition.Instance(
			context.Background(),
			driver, params,
			nil,
			"",
			false,
		)
		require.NoError(t, err)
		require.Empty(t, warnings)
		r, err := transformerCtx.Transformer.Transform(context.Background(), record)
		require.NoError(t, err)

		var res time.Time
		_, err = r.ScanColumnValueByName("date_ts", &res)
		require.NoError(t, err)
		diff := res.Sub(original)
		require.True(t, diff == 24*time.Hour || diff == -24*time.Hour, "got diff %s", diff)
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
		{
			name:       "NoiseDate with dynamic min and no max",
			columnName: "hiredate",
			record: map[string]*toolkit.RawValue{
				"hiredate":  toolkit.NewRawValue([]byte("2009-01-01 00:00:00"), false),
				"birthdate": toolkit.NewRawValue([]byte("2015-01-01 00:00:00"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max_ratio": toolkit.ParamsValue("1 day"),
				"engine":    toolkit.ParamsValue("hash"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "birthdate",
				},
			},
			expected: expected{
				min: time.Date(2014, 12, 31, 0, 0, 0, 0, time.UTC),
				max: time.Date(2015, 1, 2, 0, 0, 0, 0, time.UTC),
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
				false,
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

func runNoiseDateOnTstz(
	t *testing.T, ctx context.Context, params map[string]toolkit.ParamsValue, value string,
) time.Time {
	t.Helper()
	params["column"] = toolkit.ParamsValue("date_tstz")
	driver, record := getDriverAndRecord("date_tstz", value)
	transformerCtx, warnings, err := NoiseDateTransformerDefinition.Instance(ctx, driver, params, nil, "", false)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.NoError(t, transformerCtx.Transformer.Init(ctx))
	r, err := transformerCtx.Transformer.Transform(ctx, record)
	require.NoError(t, err)
	var res time.Time
	_, err = r.ScanColumnValueByName("date_tstz", &res)
	require.NoError(t, err)
	return res
}

func setTimeLocal(t *testing.T, name string) {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	old := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = old })
}

func TestNoiseDateTransformer_Transform_truncate_tstz_host_timezone(t *testing.T) {
	setTimeLocal(t, "America/New_York")
	params := map[string]toolkit.ParamsValue{
		"max_ratio": toolkit.ParamsValue("10 days"),
		"truncate":  toolkit.ParamsValue("day"),
	}
	res := runNoiseDateOnTstz(t, context.Background(), params, "2024-01-12 23:30:00+00")
	res = res.UTC()
	assert.Equal(t, 0, res.Hour())
	assert.Equal(t, 0, res.Minute())
	assert.Equal(t, 0, res.Second())
	assert.Equal(t, 0, res.Nanosecond())
}

func TestNoiseDateTransformer_Transform_hash_tstz_host_timezone(t *testing.T) {
	// Expected values were produced by greenmask 0.2.26 (pgx 5.10) under TZ=UTC.
	tests := []struct {
		name     string
		truncate string
		expected time.Time
	}{
		{name: "no truncate", expected: time.Date(2024, 1, 4, 7, 5, 19, 374654000, time.UTC)},
		{name: "truncate day", truncate: "day", expected: time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		for _, tz := range []string{"UTC", "America/New_York", "Europe/Berlin", "Asia/Kolkata"} {
			t.Run(tt.name+"/"+tz, func(t *testing.T) {
				setTimeLocal(t, tz)
				params := map[string]toolkit.ParamsValue{
					"max_ratio": toolkit.ParamsValue("10 days"),
					"engine":    toolkit.ParamsValue("hash"),
				}
				if tt.truncate != "" {
					params["truncate"] = toolkit.ParamsValue(tt.truncate)
				}
				ctx := commonutils.WithSalt(context.Background(), []byte("0123456789abcdef0123456789abcdef"))
				res := runNoiseDateOnTstz(t, ctx, params, "2024-01-12 23:30:00+00")
				assert.True(t, tt.expected.Equal(res), "expected %s, got %s", tt.expected, res.UTC())
			})
		}
	}
}

func TestNoiseDateTransformer_Transform_hash_tstz_common_timezone(t *testing.T) {
	// The hash seed is the instant in UTC, so common.timezone must not change the result.
	expected := time.Date(2024, 1, 4, 7, 5, 19, 374654000, time.UTC)
	for _, tz := range []string{"UTC", "Europe/Berlin", "Asia/Kolkata", "America/New_York"} {
		t.Run(tz, func(t *testing.T) {
			loc, err := time.LoadLocation(tz)
			require.NoError(t, err)
			table := &toolkit.Table{
				Schema: "public", Name: "test", Oid: 1224,
				Columns: []*toolkit.Column{
					{Name: "date_tstz", TypeName: "timestamptz", TypeOid: pgtype.TimestamptzOID, Num: 1, Length: -1},
				},
			}
			driver, _, err := toolkit.NewDriver(table, nil, toolkit.WithTimestamptzLocation(loc))
			require.NoError(t, err)
			row := pgcopy.NewRow(1)
			require.NoError(t, row.Decode([]byte("2024-01-12 23:30:00+00")))
			record := toolkit.NewRecord(driver)
			record.SetRow(row)

			ctx := commonutils.WithSalt(context.Background(), []byte("0123456789abcdef0123456789abcdef"))
			params := map[string]toolkit.ParamsValue{
				"column":    toolkit.ParamsValue("date_tstz"),
				"max_ratio": toolkit.ParamsValue("10 days"),
				"engine":    toolkit.ParamsValue("hash"),
			}
			transformerCtx, warnings, err := NoiseDateTransformerDefinition.Instance(ctx, driver, params, nil, "", false)
			require.NoError(t, err)
			require.Empty(t, warnings)
			require.NoError(t, transformerCtx.Transformer.Init(ctx))
			r, err := transformerCtx.Transformer.Transform(ctx, record)
			require.NoError(t, err)
			var res time.Time
			_, err = r.ScanColumnValueByName("date_tstz", &res)
			require.NoError(t, err)
			assert.True(t, expected.Equal(res), "expected %s, got %s", expected, res.UTC())
		})
	}
}
