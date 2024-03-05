package transformers

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimiter_Limit(t *testing.T) {
	minValue := int64(math.MinInt64)
	maxValue := int64(math.MaxInt64)
	l, err := NewInt64Limiter(minValue, maxValue)
	require.NoError(t, err)
	res := l.Limit(uint64(math.MaxInt64))
	require.True(t, res == math.MinInt64)
}
