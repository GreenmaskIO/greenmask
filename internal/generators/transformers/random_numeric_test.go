package transformers

import (
	"math/big"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestBigIntTransformer_Transform(t *testing.T) {
	sha1, err := generators.NewHash([]byte{1, 2, 3, 4}, "sha512")
	require.NoError(t, err)
	minValue, err := decimal.NewFromString("-999999999999999999999999999999999999999")
	require.NoError(t, err)
	maxValue, err := decimal.NewFromString("999999999999999999999999999999999999999")
	require.NoError(t, err)
	limiter, err := NewRandomNumericLimiter(minValue, maxValue)
	require.NoError(t, err)
	tr, err := NewRandomNumericTransformer(limiter, 0)
	require.NoError(t, err)
	err = tr.SetGenerator(sha1)
	require.NoError(t, err)
	res, err := tr.Transform([]byte("199999999999999999999999999999999999999"))
	require.NoError(t, err)
	require.True(t, res.LessThanOrEqual(maxValue) && res.GreaterThanOrEqual(minValue))
}

func TestBigFloatTransformer_Transform(t *testing.T) {
	sha1, err := generators.NewHash([]byte{1, 2, 3, 4}, "sha512")
	require.NoError(t, err)
	minValue, err := decimal.NewFromString("-1")
	require.NoError(t, err)
	maxValue, err := decimal.NewFromString("999999999999999999999999999999999999999.12345")
	require.NoError(t, err)
	limiter, err := NewRandomNumericLimiter(minValue, maxValue)
	require.NoError(t, err)
	//limiter.SetPrecision(3)
	tr, err := NewRandomNumericTransformer(limiter, 3)
	require.NoError(t, err)
	err = tr.SetGenerator(sha1)
	require.NoError(t, err)
	res, err := tr.Transform([]byte("1999999999999999999999999999999999999990"))
	require.NoError(t, err)
	require.True(t, res.LessThanOrEqual(maxValue) && res.GreaterThanOrEqual(minValue))
}

func TestRandomNumericTransformer_Transform_full_range_with_decimal(t *testing.T) {
	tests := []struct {
		name      string
		min       string
		max       string
		precision int32
	}{
		{name: "fraction only range", min: "0.5", max: "0.9", precision: 2},
		{name: "negative and positive", min: "-1000", max: "1000", precision: 2},
		{name: "positive", min: "0", max: "10", precision: 4},
		{name: "integer", min: "1", max: "3", precision: 0},
		{name: "bounds with more decimals than precision", min: "0.123", max: "0.987", precision: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minValue := decimal.RequireFromString(tt.min)
			maxValue := decimal.RequireFromString(tt.max)
			limiter, err := NewRandomNumericLimiter(minValue, maxValue)
			require.NoError(t, err)
			tr, err := NewRandomNumericTransformer(limiter, tt.precision)
			require.NoError(t, err)
			g := generators.NewRandomBytes(1, tr.GetRequiredGeneratorByteLength())
			require.NoError(t, tr.SetGenerator(g))

			distance := maxValue.Sub(minValue)
			lowQuarter := minValue.Add(distance.Div(decimal.NewFromInt(4)))
			highQuarter := maxValue.Sub(distance.Div(decimal.NewFromInt(4)))
			var low, high bool
			for i := 0; i < 1000; i++ {
				res, err := tr.Transform(nil)
				require.NoError(t, err)
				require.True(t, res.GreaterThanOrEqual(minValue) && res.LessThanOrEqual(maxValue), "value %s out of range", res)
				require.True(t, res.Equal(res.Round(tt.precision)), "value %s has more than %d decimals", res, tt.precision)
				low = low || res.LessThanOrEqual(lowQuarter)
				high = high || res.GreaterThanOrEqual(highQuarter)
			}
			require.True(t, low && high, "range is not fully covered")
		})
	}
}

func TestRandomNumericLimiter_Limit_inclusive(t *testing.T) {
	limiter, err := NewRandomNumericLimiter(decimal.NewFromInt(1), decimal.NewFromInt(3))
	require.NoError(t, err)
	require.Equal(t, "1", limiter.Limit(big.NewInt(0), 0).String())
	require.Equal(t, "3", limiter.Limit(big.NewInt(2), 0).String())
	require.Equal(t, "1", limiter.Limit(big.NewInt(3), 0).String())

	limiter, err = NewRandomNumericLimiter(decimal.NewFromInt(5), decimal.NewFromInt(5))
	require.NoError(t, err)
	require.Equal(t, "5", limiter.Limit(big.NewInt(12345), 2).String())
}
