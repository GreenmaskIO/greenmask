package transformers2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func TestJsonTransformer_Transform(t *testing.T) {
	driver := getDriver()

	transformer, warnings, err := JsonTransformerDefinition.Instance(
		context.Background(),
		driver, map[string][]byte{
			"column": []byte("doc"),
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

	originRawRecord := []string{"1", toolkit.DefaultNullSeq, "old_value", `{"name":{"last":"Anderson", "age": 5, "todelete": true}}`}

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
	require.JSONEq(t, `{"name":{"last":"Test","first":"Sara", "age": 10}}`, transformedRawRecord[3])
}
