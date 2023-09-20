package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/domains"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	var columnName = "id"
	var originalValue = "1"
	var expectedValue = "\\N"

	driver, record := getDriverAndRecord(columnName, originalValue)

	transformer, warnings, err := SetNullTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]domains.ParamsValue{
			"column": domains.ParamsValue(columnName),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	encoded, err := r.Encode()
	require.NoError(t, err)
	res, err := encoded.Encode()
	require.NoError(t, err)
	assert.Equal(t, expectedValue, string(res))
}
