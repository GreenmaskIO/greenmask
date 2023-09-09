package transformers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestRandomDateTransformer_Transform(t *testing.T) {

	tests := []struct {
		name       string
		columnName string
		params     map[string][]byte
		pattern    string
	}{
		{
			name:       "test date type",
			columnName: "date_date",
			params: map[string][]byte{
				"min": []byte("2017-09-14"),
				"max": []byte("2023-09-14"),
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name:       "test timestamp without timezone type",
			columnName: "date_ts",
			params: map[string][]byte{
				"min": []byte("2018-12-15 23:34:17.946707"),
				"max": []byte("2023-09-14 00:00:17.946707"),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name:       "test timestamp with timezone type",
			columnName: "date_tstz",
			params: map[string][]byte{
				"min": []byte("2018-12-15 23:34:17.946707+03"),
				"max": []byte("2023-09-14 00:00:17.946707+03"),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name:       "test timestamp type with Truncate till day",
			columnName: "date_ts",
			params: map[string][]byte{
				"min":      []byte("2018-12-15 23:34:17.946707"),
				"max":      []byte("2023-09-14 00:00:17.946707"),
				"truncate": []byte("month"),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, toolkit.DefaultNullSeq)
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
			res, err := r.EncodeAttr(tt.columnName)
			require.NoError(t, err)
			require.Regexp(t, tt.pattern, string(res))
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
