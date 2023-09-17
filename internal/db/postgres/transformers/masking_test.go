package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/domains"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaskingTransformer_Transform(t *testing.T) {

	tests := []struct {
		name          string
		ttype         string
		columnName    string
		originalValue string
		expectedValue string
	}{
		{
			name:          "mobile",
			ttype:         "mobile",
			columnName:    "data",
			originalValue: "+35798665784",
			expectedValue: "+357***65784",
		},
		{
			name:          "name",
			ttype:         "name",
			columnName:    "data",
			originalValue: "abcdef test",
			expectedValue: "a**def t**t",
		},
		{
			name:          "password",
			ttype:         "password",
			columnName:    "data",
			originalValue: "password_secure",
			expectedValue: "************",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)

			transformer, warnings, err := MaskingTransformerDefinition.Instance(
				context.Background(),
				driver, map[string]domains.ParamsValue{
					"column": domains.ParamsValue(tt.columnName),
					"type":   domains.ParamsValue(tt.ttype),
				},
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			res, err := r.EncodeAttr(tt.columnName)
			require.NoError(t, err)

			require.Equal(t, tt.expectedValue, string(res))
		})
	}
}

func TestMaskingTransformer_type_validation(t *testing.T) {
	var columnName = "data"
	var originalValue = "someval"
	driver, _ := getDriverAndRecord(columnName, originalValue)

	_, warnings, err := MaskingTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]domains.ParamsValue{
			"column": domains.ParamsValue(columnName),
			"type":   domains.ParamsValue("unknown"),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, warnings, 1)
	assert.Contains(t, warnings[0].Msg, "unknown type")
}
