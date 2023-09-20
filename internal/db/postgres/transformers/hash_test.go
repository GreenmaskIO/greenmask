package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashTransformer_Transform(t *testing.T) {
	var attrName = "data"
	var originalValue = "old_value"
	var expectedValue = transformers.NewValue("9n+v7qGp0ua+DgXtC9ClyjPHjWvWin6fKAmX5bZjcX4=", false)
	driver, record := getDriverAndRecord(attrName, originalValue)

	transformer, warnings, err := HashTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]domains.ParamsValue{
			"column": domains.ParamsValue(attrName),
			"salt":   domains.ParamsValue("12345678"),
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
	res, err := r.GetAttribute(attrName)
	require.NoError(t, err)

	require.Equal(t, expectedValue.IsNull, res.IsNull)
	require.Equal(t, expectedValue.Value, res.Value)
}
