package transformers

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
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

func TestGendersOrderIsStable(t *testing.T) {
	genders := NewPersonalDatabase(DefaultPersonMap).Genders
	flips := 0
	for i := 0; i < 1000; i++ {
		got := NewPersonalDatabase(DefaultPersonMap).Genders
		if got[0] != genders[0] {
			flips++
		}
	}
	if flips > 0 {
		t.Fatalf("Genders order changed in %d of 1000 builds", flips)
	}
}
