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

func TestRandomPersonTransformer_GeneratorByteLength(t *testing.T) {
	rpt := NewRandomPersonTransformer(AnyGenderName, nil)
	// 1 byte for gender and 4 bytes per attribute
	require.Equal(t, 1+rpt.GetDb().AttributesCount*4, rpt.GetRequiredGeneratorByteLength())
}

// pins outputs so the generator byte length fix does not change generated values
func TestRandomPersonTransformer_GetFullName_pinned(t *testing.T) {
	newTransformer := func(g generators.Generator) *RandomPersonTransformer {
		rpt := NewRandomPersonTransformer(AnyGenderName, nil)
		require.NoError(t, rpt.SetGenerator(g))
		return rpt
	}
	size := NewRandomPersonTransformer(AnyGenderName, nil).GetRequiredGeneratorByteLength()

	hashGen, err := generators.GetHashBytesGen([]byte("salt"), size)
	require.NoError(t, err)
	res, err := newTransformer(hashGen).GetFullName("", []byte("John Doe"))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"FirstName": "Verona", "LastName": "Grimes", "Title": "Queen"}, res)

	res, err = newTransformer(generators.NewRandomBytes(1, size)).GetFullName("", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"FirstName": "Eveline", "LastName": "Toy", "Title": "Queen"}, res)
}
