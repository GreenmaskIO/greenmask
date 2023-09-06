package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	driver := getDriver()

	transformer, warnings, err := SetNullTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte("id"),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	originRawRecord := []string{"1", toolkit.DefaultNullSeq, "old_value"}

	r, err := transformer.Transform(
		context.Background(),
		toolkit.NewRecord(
			driver,
			originRawRecord,
		),
	)
	require.NoError(t, err)
	transformedRawRecord, err := r.Encode()
	require.NoError(t, err)

	require.Equal(t, transformedRawRecord[0], toolkit.DefaultNullSeq)
}
