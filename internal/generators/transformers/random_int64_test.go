package transformers

import (
	"math"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestLimiter_Limit(t *testing.T) {
	minValue := int64(math.MinInt64)
	maxValue := int64(math.MaxInt64)
	l, err := NewInt64Limiter(minValue, maxValue)
	require.NoError(t, err)
	res := l.Limit(uint64(math.MaxUint64 - 1))
	require.True(t, res == math.MaxInt64-1)
}

func TestLimiter_negative_Limit(t *testing.T) {
	minValue := int64(-10000)
	maxValue := int64(-1)
	l, err := NewInt64Limiter(minValue, maxValue)
	require.NoError(t, err)
	res := l.Limit(100000000)
	log.Debug().Int64("res", res).Msg("")
	require.True(t, res == -10000)
}

func TestLimiter_Limit_inclusive_range(t *testing.T) {
	tests := []struct {
		name     string
		min      int64
		max      int64
		expected []int64
	}{
		{name: "positive", min: 1, max: 3, expected: []int64{1, 2, 3}},
		{name: "negative", min: -5, max: -1, expected: []int64{-5, -4, -3, -2, -1}},
		{name: "cross zero", min: -1, max: 1, expected: []int64{-1, 0, 1}},
		{name: "min equal max", min: 5, max: 5, expected: []int64{5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := NewInt64Limiter(tt.min, tt.max)
			require.NoError(t, err)
			seen := make(map[int64]struct{})
			for v := uint64(0); v < 100; v++ {
				res := l.Limit(v)
				require.GreaterOrEqual(t, res, tt.min)
				require.LessOrEqual(t, res, tt.max)
				seen[res] = struct{}{}
			}
			for _, e := range tt.expected {
				require.Contains(t, seen, e)
			}
		})
	}
}

func TestLimiter_Limit_full_range(t *testing.T) {
	l, err := NewInt64Limiter(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), l.Limit(0))
	require.Equal(t, int64(math.MaxInt64), l.Limit(math.MaxUint64))

	l, err = NewInt64Limiter(math.MinInt64+1, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64+1), l.Limit(0))
	require.Equal(t, int64(math.MaxInt64), l.Limit(math.MaxUint64-1))
}

func TestNewInt64Limiter_wrong_limits(t *testing.T) {
	_, err := NewInt64Limiter(2, 1)
	require.ErrorIs(t, err, ErrWrongLimits)
}
