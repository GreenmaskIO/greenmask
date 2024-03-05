package transformers

import (
	"context"
	"testing"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestBigIntTransformer_Transform(t *testing.T) {
	sha1 := generators.NewSha1([]byte{1, 2, 3, 4})
	minValue, err := decimal.NewFromString("-999999999999999999999999999999999999999")
	maxValue, err := decimal.NewFromString("999999999999999999999999999999999999999")
	require.NoError(t, err)
	limiter, err := NewBigIntLimiter(minValue, maxValue)
	require.NoError(t, err)
	tr, err := NewBigIntTransformer(sha1, limiter)
	require.NoError(t, err)
	resBytes, err := tr.Transform(context.Background(), []byte("199999999999999999999999999999999999999"))
	require.NoError(t, err)
	res, err := decimal.NewFromString(string(resBytes))
	require.NoError(t, err)
	require.True(t, res.LessThanOrEqual(maxValue) && res.GreaterThanOrEqual(minValue))
}
