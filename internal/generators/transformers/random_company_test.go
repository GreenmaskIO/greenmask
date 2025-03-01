package transformers

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestRandomCompanyNameTransformer_GetCompanyName(t *testing.T) {
	rnt := NewRandomCompanyTransformer(nil)
	g := generators.NewRandomBytes(time.Now().UnixNano(), rnt.GetRequiredGeneratorByteLength())
	err := rnt.SetGenerator(g)
	require.NoError(t, err)
	res, err := rnt.GetCompanyName([]byte{})
	require.NoError(t, err)
	require.True(t, slices.Contains(DefaultCompanyNames, res["CompanyName"]))
}
