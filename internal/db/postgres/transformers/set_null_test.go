package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/domains"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	var columnName = "id"
	var originalValue = "1"
	var expectedValue = toolkit.DefaultNullSeq

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
	res, err := r.EncodeAttr(columnName)
	require.NoError(t, err)

	require.Equal(t, expectedValue, string(res))
}
