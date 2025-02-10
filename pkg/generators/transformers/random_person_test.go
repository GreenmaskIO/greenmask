package transformers

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

func TestRandomNameTransformer_GetFullName(t *testing.T) {
	rnt := NewRandomPersonTransformer(AnyGenderName, nil)
	g := generators.NewRandomBytes(time.Now().UnixNano(), rnt.GetRequiredGeneratorByteLength())
	err := rnt.SetGenerator(g)
	require.NoError(t, err)
	res, err := rnt.GetFullName("", []byte{})
	require.NoError(t, err)
	require.True(t, slices.Contains(DefaultFirstNamesMale, res["FirstName"]) || slices.Contains(DefaultFirstNamesFemale, res["FirstName"]))
	require.True(t, slices.Contains(DefaultLastNames, res["LastName"]))
}
