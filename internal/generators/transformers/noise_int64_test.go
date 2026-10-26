package transformers

import (
	"math"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestNoiseInt64Transformer_Transform(t *testing.T) {
	minVal := int64(-1000)
	maxVal := int64(100)
	l, err := NewNoiseInt64Limiter(minVal, maxVal)
	require.NoError(t, err)
	tr, err := NewNoiseInt64Transformer(l, 0.1, 0.9)
	require.NoError(t, err)
	g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform(nil, 17)
	require.NoError(t, err)
	log.Debug().Int64("value", res).Msg("")
	require.True(t, res >= minVal && res <= maxVal)
}

func TestNoiseInt64Transformer_Transform_no_overflow(t *testing.T) {
	l, err := NewNoiseInt64Limiter(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	tr, err := NewNoiseInt64Transformer(l, 0.2, 0.3)
	require.NoError(t, err)
	g := generators.NewRandomBytes(1, tr.GetRequiredGeneratorByteLength())
	require.NoError(t, tr.SetGenerator(g))
	for _, original := range []int64{8000000000000000000, -8000000000000000000} {
		for i := 0; i < 50; i++ {
			res, err := tr.Transform(nil, original)
			require.NoError(t, err)
			delta := float64(res) - float64(original)
			// Noise is at most 30% of the value, a wrapped result is far away.
			require.LessOrEqual(t, math.Abs(delta), math.Abs(float64(original))*0.31, "original %d result %d", original, res)
		}
	}
}

func TestAddNoiseSaturated(t *testing.T) {
	require.Equal(t, int64(math.MaxInt64), addNoiseSaturated(math.MaxInt64-1, 10))
	require.Equal(t, int64(math.MinInt64), addNoiseSaturated(math.MinInt64+1, -10))
	require.Equal(t, int64(math.MaxInt64), addNoiseSaturated(1, 1e30))
	require.Equal(t, int64(math.MinInt64), addNoiseSaturated(-1, -1e30))
	require.Equal(t, int64(110), addNoiseSaturated(100, 10.9))
}
