package transformers

import (
	"math"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestNewFloat64Transformer(t *testing.T) {
	limiter, err := NewFloat64Limiter(-1, 1, 2)
	require.NoError(t, err)
	tr := NewRandomFloat64Transformer(limiter)
	g, err := generators.NewHash([]byte{}, "sha1")
	require.NoError(t, err)
	g = generators.NewHashReducer(g, tr.GetRequiredGeneratorByteLength())
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform(nil, []byte{})
	require.NoError(t, err)
	log.Debug().Msgf("value = %f", res)
	require.True(t, res >= -1 && res <= 1)
}

func TestRound_large_values(t *testing.T) {
	require.Equal(t, 1e16, Round(4, 1e16))
	require.Equal(t, -1e300, Round(2, -1e300))
	require.Equal(t, 123.46, Round(2, 123.456))
	require.Equal(t, -123.46, Round(2, -123.456))
}

func TestFloat64Limiter_Limit_wide_range(t *testing.T) {
	tests := []struct {
		name string
		min  float64
		max  float64
	}{
		{name: "float8 full range", min: -math.MaxFloat64, max: math.MaxFloat64},
		{name: "big positive", min: 0, max: 1e16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := NewFloat64Limiter(tt.min, tt.max, 4)
			require.NoError(t, err)
			for _, v := range []float64{0, 0.25, 0.5, 0.99, 1} {
				res := l.Limit(v)
				require.False(t, math.IsInf(res, 0) || math.IsNaN(res), "got %f", res)
				require.True(t, res >= tt.min && res <= tt.max, "got %f", res)
			}
			require.Equal(t, tt.max, l.Limit(1))
			require.Equal(t, tt.min, l.Limit(0))
		})
	}
}

func TestFloat64Limiter_Limit_rounding_within_bounds(t *testing.T) {
	l, err := NewFloat64Limiter(0.004, 1.005, 2)
	require.NoError(t, err)
	require.Equal(t, 1.0, l.Limit(1))
	require.Equal(t, 0.01, l.Limit(0))
}
