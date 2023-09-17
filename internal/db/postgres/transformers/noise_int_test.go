package transformers

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO: Test the max/min value exceeded
func TestNoiseIntTransformer_Transform(t *testing.T) {

	type result struct {
		min     int64
		max     int64
		pattern string
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
			result:        result{min: 10, max: 190, pattern: `^-*\d+$`},
			originalValue: "123",
		},
		{
			name:          "int4",
			columnName:    "id4",
			ratio:         0.9,
			result:        result{min: 10, max: 190, pattern: `^-*\d+$`},
			originalValue: "123",
		},
		{
			name:          "int8",
			columnName:    "id8",
			ratio:         0.9,
			result:        result{min: 10, max: 190, pattern: `^-*\d+$`},
			originalValue: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := NoiseIntTransformerDefinition.Instance(
				context.Background(),
				driver, map[string]domains.ParamsValue{
					"column": domains.ParamsValue(tt.columnName),
					"ratio":  domains.ParamsValue(fmt.Sprintf("%f", tt.ratio)),
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
			require.Regexp(t, tt.result.pattern, string(res))
		})
	}
}
