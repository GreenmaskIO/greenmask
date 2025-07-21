package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/utils"
)

func Test_getGenerateEngine(t *testing.T) {
	t.Run("random", func(t *testing.T) {
		ctx := context.Background()
		g, err := getGenerateEngine(ctx, RandomEngineParameterName, 32)
		require.NoError(t, err)
		a, err := g.Generate([]byte("123"))
		require.NoError(t, err)
		b, err := g.Generate([]byte("123"))
		require.NoError(t, err)
		require.NotEqual(t, a, b)
	})

	t.Run("random with the same salt", func(t *testing.T) {
		salt := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		ctx := utils.WithSalt(context.Background(), salt)

		g1, err := getGenerateEngine(ctx, HashEngineParameterName, 32)
		require.NoError(t, err)
		a, err := g1.Generate([]byte("123"))
		require.NoError(t, err)

		g2, err := getGenerateEngine(ctx, HashEngineParameterName, 32)
		require.NoError(t, err)
		b, err := g2.Generate([]byte("123"))
		require.NoError(t, err)

		require.Equal(t, a, b)
	})

	t.Run("random with different salt", func(t *testing.T) {
		salt1 := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		ctx1 := utils.WithSalt(context.Background(), salt1)

		g1, err := getGenerateEngine(ctx1, HashEngineParameterName, 32)
		require.NoError(t, err)
		a, err := g1.Generate([]byte("123"))
		require.NoError(t, err)

		salt2 := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		ctx2 := utils.WithSalt(context.Background(), salt2)
		g2, err := getGenerateEngine(ctx2, HashEngineParameterName, 32)
		require.NoError(t, err)
		b, err := g2.Generate([]byte("123"))
		require.NoError(t, err)

		require.NotEqual(t, a, b)
	})
}
