package transformers

import (
	"testing"

	"github.com/greenmaskio/greenmask/pkg/generators"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestChoiceTransformer_Transform(t *testing.T) {
	data := []*toolkit.RawValue{
		{Data: []byte("a")},
		{Data: []byte("b")},
	}
	tr := NewRandomChoiceTransformer(data)
	g, err := generators.NewHash([]byte{}, "sha1")
	require.NoError(t, err)
	g = generators.NewHashReducer(g, tr.GetRequiredGeneratorByteLength())
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform([]byte{})
	require.NoError(t, err)
	require.Contains(t, data, res)
}
