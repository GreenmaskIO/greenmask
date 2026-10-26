package transformers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoiseFloat64Limiter_Limit_rounds_clamped_values(t *testing.T) {
	l, err := NewNoiseFloat64Limiter(100.55, 300.123, 1)
	require.NoError(t, err)
	require.Equal(t, 300.1, l.Limit(500))
	require.Equal(t, 100.6, l.Limit(1))
	require.Equal(t, 123.5, l.Limit(123.456))

	// Rounding of an in-range value must not leave the range.
	l, err = NewNoiseFloat64Limiter(0, 300.12, 1)
	require.NoError(t, err)
	require.Equal(t, 300.1, l.Limit(300.119))
}

func TestNoiseFloat64Limiter_min_equal_max(t *testing.T) {
	l, err := NewNoiseFloat64Limiter(5, 5, 2)
	require.NoError(t, err)
	require.Equal(t, 5.0, l.Limit(100))
}
