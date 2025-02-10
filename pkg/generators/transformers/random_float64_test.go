package transformers

import (
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/generators"
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
