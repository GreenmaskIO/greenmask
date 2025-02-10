package transformers

import (
	"testing"

	"github.com/greenmaskio/greenmask/pkg/generators"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestUuidTransformer_Transform_hash(t *testing.T) {
	regexp := `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`

	ut := NewRandomUuidTransformer()
	hashFuncName, _, err := generators.GetHashFunctionNameBySize(ut.GetRequiredGeneratorByteLength())
	require.NoError(t, err)
	g, err := generators.NewHash([]byte{}, hashFuncName)
	require.NoError(t, err)
	g = generators.NewHashReducer(g, uuidTransformerRequiredLength)
	err = ut.SetGenerator(g)
	require.NoError(t, err)
	res, err := ut.Transform([]byte{})
	require.NoError(t, err)
	resStr, err := res.MarshalText()
	require.NoError(t, err)
	require.Regexp(t, regexp, string(resStr))
	log.Debug().Str("value", string(resStr)).Msg("")
}
