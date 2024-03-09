package generators

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytesRandom_Generate(t *testing.T) {
	r := NewRandomBytes(0, 3)
	res, err := r.Generate(nil)
	require.NoError(t, err)
	require.Len(t, res, 3)
	require.Equal(t, []byte{1, 148, 253}, res)
}
