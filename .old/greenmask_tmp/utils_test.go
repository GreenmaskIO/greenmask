package transformers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTruncateDate(t *testing.T) {
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
