package transformers

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestNoiseNumericLimiter_Limit_rounds_clamped_values(t *testing.T) {
	l, err := NewNoiseNumericLimiter(decimal.RequireFromString("10.45"), decimal.RequireFromString("100.55"))
	require.NoError(t, err)
	l.SetPrecision(0)
	require.Equal(t, "100", l.Limit(decimal.RequireFromString("123.456")).String())
	require.Equal(t, "11", l.Limit(decimal.RequireFromString("1")).String())
	require.Equal(t, "50", l.Limit(decimal.RequireFromString("49.5")).String())
	// Rounding of an in-range value must not leave the range.
	require.Equal(t, "100", l.Limit(decimal.RequireFromString("100.54")).String())
}

func TestNoiseNumericLimiter_min_equal_max(t *testing.T) {
	l, err := NewNoiseNumericLimiter(decimal.NewFromInt(5), decimal.NewFromInt(5))
	require.NoError(t, err)
	require.Equal(t, "5", l.Limit(decimal.NewFromInt(100)).String())
}
