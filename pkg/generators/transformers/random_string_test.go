package transformers

import (
	"testing"

	"github.com/greenmaskio/greenmask/pkg/generators"
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
	res := st.Transform([]byte{})
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
	res := st.Transform([]byte{})
	log.Debug().Str("value", string(res)).Msg("")
	require.True(t, len(res) >= 10 && len(res) <= 100)
	require.Equal(t, "xvz16-K2SEYfw~rMwctQfflfq3rAHtLyyYNppFhYXrNyw027~L3TFZgxAsNxduRggmgr4sBIuMzzZOqqGiZYsOzx138AM4UGahy", string(res))
}
