package transformers

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				"ratio":  toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"column": toolkit.ParamsValue("date_date"),
			},
			original: "2023-06-25",
			result: result{
				pattern: `^\d{4}-\d{2}-\d{2}$`,
				min:     time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max:     time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp without timezone type",
			params: map[string]toolkit.ParamsValue{
				"ratio":  toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"column": toolkit.ParamsValue("date_ts"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
				min:     time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max:     time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp with timezone type",
			params: map[string]toolkit.ParamsValue{
				"ratio":  toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"column": toolkit.ParamsValue("date_tstz"),
			},
			original: "2023-06-25 00:00:00.0+03",
			result: result{
				pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
				min:     time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max:     time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
		},
		{
			name: "test timestamp type with Truncate till day",
			params: map[string]toolkit.ParamsValue{
				"ratio":    toolkit.ParamsValue("1 year 1 mons 1 day 01:01:01.01"),
				"truncate": toolkit.ParamsValue("month"),
				"column":   toolkit.ParamsValue("date_ts"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
				min:     time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max:     time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(string(tt.params["column"]), tt.original)
			transformer, warnings, err := NoiseDateTransformerDefinition.Instance(
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

			res, err := r.GetAttribute(string(tt.params["column"]))
			require.NoError(t, err)
			// Checking typed value
			assert.False(t, res.IsNull)
			resTime, ok := res.Value.(time.Time)
			require.True(t, ok)
			assert.WithinRangef(t, resTime, tt.result.min, tt.result.max, "result is not in range")

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
