package transformers

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestBigIntTransformer_Transform(t *testing.T) {
	sha1, err := generators.NewHash([]byte{1, 2, 3, 4}, "sha1")
	require.NoError(t, err)
	minValue, err := decimal.NewFromString("-999999999999999999999999999999999999999")
	maxValue, err := decimal.NewFromString("999999999999999999999999999999999999999")
	require.NoError(t, err)
	limiter, err := NewDecimalLimiter(minValue, maxValue)
	require.NoError(t, err)
	tr, err := NewDecimalTransformer(limiter, 0)
	require.NoError(t, err)
	err = tr.SetGenerator(sha1)
	require.NoError(t, err)
	res, err := tr.Transform(context.Background(), []byte("199999999999999999999999999999999999999"))
	require.NoError(t, err)
	require.True(t, res.LessThanOrEqual(maxValue) && res.GreaterThanOrEqual(minValue))
}

func TestBigFloatTransformer_Transform(t *testing.T) {
	sha1, err := generators.NewHash([]byte{1, 2, 3, 4}, "sha1")
	require.NoError(t, err)
	minValue, err := decimal.NewFromString("-1")
	maxValue, err := decimal.NewFromString("999999999999999999999999999999999999999.12345")
	require.NoError(t, err)
	limiter, err := NewDecimalLimiter(minValue, maxValue)
	require.NoError(t, err)
	//limiter.SetPrecision(3)
	tr, err := NewDecimalTransformer(limiter, 3)
	require.NoError(t, err)
	err = tr.SetGenerator(sha1)
	require.NoError(t, err)
	res, err := tr.Transform(context.Background(), []byte("1999999999999999999999999999999999999990"))
	require.NoError(t, err)
	require.True(t, res.LessThanOrEqual(maxValue) && res.GreaterThanOrEqual(minValue))
}
