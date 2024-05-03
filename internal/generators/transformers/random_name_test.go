package transformers

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestRandomNameTransformer_GetFullName(t *testing.T) {
	rnt := NewRandomNameTransformer(AnyGenderName, RandomFullNameTransformerFullNameMode)
	g := generators.NewRandomBytes(time.Now().UnixNano(), rnt.GetRequiredGeneratorByteLength())
	err := rnt.SetGenerator(g)
	require.NoError(t, err)
	res, err := rnt.GetFullName(context.Background(), []byte{})
	require.NoError(t, err)
	require.True(t, slices.Contains(DefaultFirstNamesMale, res.FirstName) || slices.Contains(DefaultFirstNamesFemale, res.FirstName))
	require.True(t, slices.Contains(DefaultLastNames, res.LastName))
	require.True(t, slices.Contains(allowedGenderNames, res.Gender))
}

func TestRandomFullNameTransformer_GetFirstName(t *testing.T) {
	rnt := NewRandomNameTransformer(AnyGenderName, RandomFullNameTransformerFullNameMode)
	g := generators.NewRandomBytes(time.Now().UnixNano(), rnt.GetRequiredGeneratorByteLength())
	err := rnt.SetGenerator(g)
	require.NoError(t, err)
	res, err := rnt.GetFirstName(context.Background(), []byte{})
	require.NoError(t, err)
	require.True(t, slices.Contains(DefaultFirstNamesMale, res) || slices.Contains(DefaultFirstNamesFemale, res))
}

func TestRandomFullNameTransformer_GetLastName(t *testing.T) {
	rnt := NewRandomNameTransformer(AnyGenderName, RandomFullNameTransformerFullNameMode)
	g := generators.NewRandomBytes(time.Now().UnixNano(), rnt.GetRequiredGeneratorByteLength())
	err := rnt.SetGenerator(g)
	require.NoError(t, err)
	res, err := rnt.GetLastName([]byte{})
	require.NoError(t, err)
	require.True(t, slices.Contains(DefaultLastNames, res))
}
