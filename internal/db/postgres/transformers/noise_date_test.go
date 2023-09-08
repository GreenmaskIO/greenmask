package transformers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoiseDateTransformer_Transform(t *testing.T) {
	loc := time.Now().Location()

	type result struct {
		min, max time.Time
	}

	tests := []struct {
		name     string
		params   map[string][]byte
		original string
		result   result
		pattern  string
	}{
		{
			name: "test date type",
			params: map[string][]byte{
				"ratio":  []byte("1 year 1 mons 1 day 01:01:01.01"),
				"column": []byte("date_date"),
			},
			original: "2023-06-25",
			result: result{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-\d{2}$`,
		},
		{
			name: "test timestamp without timezone type",
			params: map[string][]byte{
				"ratio":  []byte("1 year 1 mons 1 day 01:01:01.01"),
				"column": []byte("date_ts"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}$`,
		},
		{
			name: "test timestamp with timezone type",
			params: map[string][]byte{
				"ratio":  []byte("1 year 1 mons 1 day 01:01:01.01"),
				"column": []byte("date_tstz"),
			},
			original: "2023-06-25 00:00:00.0+03",
			result: result{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{1,6}Z$`,
		},
		{
			name: "test timestamp type with Truncate till day",
			params: map[string][]byte{
				"ratio":    []byte("1 year 1 mons 1 day 01:01:01.01"),
				"truncate": []byte("month"),
				"column":   []byte("date_ts"),
			},
			original: "2023-06-25 00:00:00",
			result: result{
				min: time.Date(2023, 5, 25, 0, 0, 0, 0, loc),
				max: time.Date(2024, 6, 26, 1, 1, 1, 1000, loc),
			},
			pattern: `^\d{4}-\d{2}-01 0{2}:0{2}:0{2}$`,
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
			res, err := r.EncodeAttr(string(tt.params["column"]))
			require.NoError(t, err)
			require.Regexp(t, tt.pattern, string(res))

			resAny, err := r.GetAttribute(string(tt.params["column"]))
			require.NoError(t, err)
			resTime, ok := resAny.(time.Time)
			require.True(t, ok)
			assert.WithinRangef(t, resTime, tt.result.min, tt.result.max, "result is not in range")
		})
	}
}
