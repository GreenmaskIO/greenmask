package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

// TODO: Test the max/min value exceeded
func TestNoiseIntTransformer_Transform(t *testing.T) {

	type result struct {
		min    int64
		max    int64
		isNull bool
	}

	// Positive cases
	tests := []struct {
		name          string
		ratio         float64
		columnName    string
		originalValue string
		result        result
	}{
		{
			name:          "int2",
			columnName:    "id2",
			ratio:         0.9,
			result:        result{min: 10, max: 190},
			originalValue: "123",
		},
		{
			name:          "int4",
			columnName:    "id4",
			ratio:         0.9,
			result:        result{min: 10, max: 190},
			originalValue: "123",
		},
		{
			name:          "int8",
			columnName:    "id8",
			ratio:         0.9,
			result:        result{min: 10, max: 190},
			originalValue: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := NoiseIntTransformerDefinition.Instance(
				context.Background(),
				driver, map[string]toolkit.ParamsValue{
					"column": toolkit.ParamsValue(tt.columnName),
					"ratio":  toolkit.ParamsValue(fmt.Sprintf("%f", tt.ratio)),
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

			val, err := r.GetAttribute(tt.columnName)
			require.NoError(t, err)
			require.Equal(t, tt.result.isNull, val.IsNull)
			if !tt.result.isNull {
				intValue := val.Value.(*int64)
				assert.GreaterOrEqual(t, *intValue, tt.result.min)
				assert.LessOrEqual(t, *intValue, tt.result.max)
			}
		})
	}
}
