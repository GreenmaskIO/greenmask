package transformers

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomDateTransformer_Transform(t *testing.T) {

	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string]domains.ParamsValue
		pattern    string
		isNull     bool
	}{
		{
			name:       "test date type",
			columnName: "date_date",
			original:   "2007-09-14",
			params: map[string]domains.ParamsValue{
				"min": domains.ParamsValue("2017-09-14"),
				"max": domains.ParamsValue("2023-09-14"),
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name:       "test timestamp without timezone type",
			columnName: "date_ts",
			original:   "2008-12-15 23:34:17.946707",
			params: map[string]domains.ParamsValue{
				"min": domains.ParamsValue("2018-12-15 23:34:17.946707"),
				"max": domains.ParamsValue("2023-09-14 00:00:17.946707"),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name:       "test timestamp with timezone type",
			columnName: "date_tstz",
			original:   "2008-12-15 23:34:17.946707+03",
			params: map[string]domains.ParamsValue{
				"min": domains.ParamsValue("2018-12-15 00:00:00.946707+03"),
				"max": domains.ParamsValue("2023-09-14 00:00:17.946707+03"),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name:       "test timestamp type with Truncate till day",
			columnName: "date_ts",
			original:   "2008-12-15 23:34:17.946707",
			params: map[string]domains.ParamsValue{
				"min":      domains.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":      domains.ParamsValue("2023-09-14 00:00:17.946707"),
				"truncate": domains.ParamsValue("month"),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
		{
			name:       "keep_null false and NULL seq",
			columnName: "date_ts",
			original:   "\\N",
			params: map[string]domains.ParamsValue{
				"min":       domains.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":       domains.ParamsValue("2023-09-14 00:00:17.946707"),
				"truncate":  domains.ParamsValue("month"),
				"keep_null": domains.ParamsValue("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, "\\N"),
			isNull:  true,
		},
		{
			name:       "keep_null true and NULL seq",
			columnName: "date_ts",
			original:   "\\N",
			params: map[string]domains.ParamsValue{
				"min":       domains.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":       domains.ParamsValue("2023-09-14 00:00:17.946707"),
				"truncate":  domains.ParamsValue("month"),
				"keep_null": domains.ParamsValue("false"),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = domains.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RandomDateTransformerDefinition.Instance(
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

func TestRandomDateTruncateDate(t *testing.T) {
	loc := time.Now().Location()
	tests := []struct {
		name     string
		part     string
		original time.Time
		expected time.Time
	}{
		{
			name:     "nano",
			part:     "nano",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
		},
		{
			name:     "second",
			part:     "second",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 56, 35, 0, loc),
		},
		{
			name:     "minute",
			part:     "minute",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 56, 0, 0, loc),
		},
		{
			name:     "hour",
			part:     "hour",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 0, 0, 0, loc),
		},
		{
			name:     "day",
			part:     "day",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 0, 0, 0, 0, loc),
		},
		{
			name:     "month",
			part:     "month",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 1, 0, 0, 0, 0, loc),
		},
		{
			name:     "year",
			part:     "year",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 1, 1, 0, 0, 0, 0, loc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := truncateDate(&tt.original, &tt.part)
			assert.Equal(t, tt.expected, res)
		})
	}
}
