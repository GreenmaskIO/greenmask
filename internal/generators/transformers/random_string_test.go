package transformers

import (
	"errors"
	"testing"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestStringTransformer_Transform_hash(t *testing.T) {
	st, err := NewRandomStringTransformer([]rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._\\-~"), 10, 100)
	require.NoError(t, err)

	hashFuncName, _, err := generators.GetHashFunctionNameBySize(st.GetRequiredGeneratorByteLength())
	require.NoError(t, err)
	g, err := generators.NewHash([]byte{}, hashFuncName)
	require.NoError(t, err)
	err = st.SetGenerator(g)
	require.NoError(t, err)
	res, err := st.Transform([]byte{})
	require.NoError(t, err)
	log.Debug().Str("value", string(res)).Msg("")
	require.True(t, len(res) >= 10 && len(res) <= 100)
	require.Equal(t, "-bM6BQ6~uJ", string(res))
}

func TestStringTransformer_Transform_random(t *testing.T) {
	st, err := NewRandomStringTransformer([]rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._\\-~"), 10, 100)
	require.NoError(t, err)

	g := generators.NewRandomBytes(0, st.GetRequiredGeneratorByteLength())
	err = st.SetGenerator(g)
	require.NoError(t, err)
	res, err := st.Transform([]byte{})
	require.NoError(t, err)
	log.Debug().Str("value", string(res)).Msg("")
	require.True(t, len(res) >= 10 && len(res) <= 100)
	require.Equal(t, "xvz16-K2SEYfw~rMwctQfflfq3rAHtLyyYNppFhYXrNyw027~L3TFZgxAsNxduRggmgr4sBIuMzzZOqqGiZYsOzx138AM4UGahy", string(res))
}

func TestStringTransformer_Transform_more_than_256_symbols(t *testing.T) {
	symbols := make([]rune, 0, 300)
	for i := 0; i < 300; i++ {
		symbols = append(symbols, rune(0x0400+i))
	}
	tests := []struct {
		length   int
		expected string
	}{
		// Outputs that were produced before the fix must stay the same
		{length: 5, expected: "ӐґйԏФ"},
		// These lengths used to panic
		{length: 10, expected: "ӐґйԏФџӑҒкԐ"},
		{length: 40, expected: "҆һӮАӡԠҋЁйѪӔ҇ҼӯБӢԡҌЂкѫӕ҈ҽӰВӣԢҍЃлѬӖ҉ҾӱГӤԣҎ"},
	}
	for _, tt := range tests {
		st, err := NewRandomStringTransformer(symbols, tt.length, tt.length)
		require.NoError(t, err)
		hashFuncName, _, err := generators.GetHashFunctionNameBySize(st.GetRequiredGeneratorByteLength())
		require.NoError(t, err)
		g, err := generators.NewHash([]byte("salt"), hashFuncName)
		require.NoError(t, err)
		require.NoError(t, st.SetGenerator(g))
		res, err := st.Transform([]byte("data"))
		require.NoError(t, err)
		require.Equal(t, tt.expected, string(res))
	}
}

type failingGenerator struct{}

func (failingGenerator) Generate([]byte) ([]byte, error) { return nil, errors.New("boom") }
func (failingGenerator) Size() int                       { return 64 }

func TestStringTransformer_Transform_generator_error(t *testing.T) {
	st, err := NewRandomStringTransformer([]rune("abc"), 1, 10)
	require.NoError(t, err)
	require.NoError(t, st.SetGenerator(failingGenerator{}))
	_, err = st.Transform([]byte("data"))
	require.Error(t, err)
}
