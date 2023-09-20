package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonTransformer_Transform(t *testing.T) {
	var attrName = "doc"
	var originalValue = `{"name":{"last":"Anderson", "age": 5, "todelete": true}}`
	var expectedValue = transformers.NewValue(`{"name":{"last":"Test","first":"Sara", "age": 10}}`, false)
	driver, record := getDriverAndRecord(attrName, originalValue)
	transformer, warnings, err := JsonTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]domains.ParamsValue{
			"column": domains.ParamsValue(attrName),
			"operations": domains.ParamsValue(`[
				{"operation": "set", "path": "name.first", "value": "Sara"},
				{"operation": "set", "path": "name.last", "value": "Test"},
				{"operation": "set", "path": "name.age", "value": 10},
				{"operation": "delete", "path": "name.todelete"}
			]`),
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
	expected := expectedValue.Value.(string)
	resValue := res.Value.(string)
	require.JSONEq(t, expected, resValue)
}
