package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexpReplaceTransformer_Transform(t *testing.T) {
	var columnName = "data"
	var original = "Hello world!!!?"
	var expected = "Hello Mr NoName !!!?"
	var params = map[string][]byte{
		"column":  []byte(columnName),
		"regexp":  []byte(`(Hello)\s*world\s*(\!+\?)`),
		"replace": []byte("$1 Mr NoName $2"),
	}
	driver, record := getDriverAndRecord(columnName, original)

	transformer, warnings, err := RegexpReplaceTransformerDefinition.Instance(
		context.Background(),
		driver, params,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.EncodeAttr(columnName)
	require.NoError(t, err)
	require.Equal(t, expected, string(res))

}
