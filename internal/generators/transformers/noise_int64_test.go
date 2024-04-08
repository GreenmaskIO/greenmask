package transformers

import (
	"context"
	"testing"
	"time"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
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
	res, err := tr.Transform(context.Background(), 17)
	require.NoError(t, err)
	log.Debug().Int64("value", res).Msg("")
	require.True(t, res >= minVal && res <= maxVal)
}
