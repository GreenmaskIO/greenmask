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
	require.True(t, res == -9999)
}
