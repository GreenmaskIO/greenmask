package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonTransformer_Transform(t *testing.T) {
	var attrName = "doc"
	var originalValue = `{"name":{"last":"Anderson", "age": 5, "todelete": true}}`
	var expectedValue = `{"name":{"last":"Test","first":"Sara", "age": 10}}`
	driver, record := getDriverAndRecord(attrName, originalValue)
	transformer, warnings, err := JsonTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte(attrName),
			"operations": []byte(`[
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
	res, err := r.EncodeAttr(attrName)
	require.NoError(t, err)
	require.JSONEq(t, expectedValue, string(res))
}
