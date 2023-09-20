package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/domains"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomFloatTransformer_Transform(t *testing.T) {
	type result struct {
		min    float64
		max    float64
		isNull bool
	}

	tests := []struct {
		name          string
		params        map[string]domains.ParamsValue
		columnName    string
		originalValue string
		result        result
	}{
		{
			name:          "float4",
			columnName:    "col_float4",
			originalValue: "1000.0",
			params: map[string]domains.ParamsValue{
				"min": domains.ParamsValue("1"),
				"max": domains.ParamsValue("10"),
			},
			result: result{
				min: 1,
				max: 10,
			},
		},
		{
			name:          "float8",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string]domains.ParamsValue{
				"min": domains.ParamsValue("1"),
				"max": domains.ParamsValue("10"),
			},
			result: result{
				min: 1,
				max: 10,
			},
		},
		{
			name:          "float8 ranges 1",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string]domains.ParamsValue{
				"min":       domains.ParamsValue("-100000"),
				"max":       domains.ParamsValue("100000"),
				"precision": domains.ParamsValue("10"),
			},
			result: result{
				min: -100000,
				max: 100000,
			},
		},
		{
			name:          "float8 ranges 1 with precision",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string]domains.ParamsValue{
				"min":       domains.ParamsValue("-100000"),
				"max":       domains.ParamsValue("-1"),
				"precision": domains.ParamsValue("0"),
			},
			result: result{
				min: -100000,
				max: -1,
			},
		},
		{
			name:          "keep_null false and NULL seq",
			columnName:    "col_float8",
			originalValue: "\\N",
			params: map[string]domains.ParamsValue{
				"min":       domains.ParamsValue("-100000"),
				"max":       domains.ParamsValue("-1"),
				"precision": domains.ParamsValue("0"),
				"keep_null": domains.ParamsValue("false"),
			},
			result: result{
				min: -100000,
				max: -1,
			},
		},
		{
			name:          "keep_null true and NULL seq",
			columnName:    "col_float8",
			originalValue: "\\N",
			params: map[string]domains.ParamsValue{
				"min":       domains.ParamsValue("-100000"),
				"max":       domains.ParamsValue("-1"),
				"precision": domains.ParamsValue("0"),
				"keep_null": domains.ParamsValue("true"),
			},
			result: result{
				isNull: true,
			},
		},
		//{
		//	name: "text with default float8",
		//	params: map[string]domains.ParamsValue{
		//		"min":       domains.ParamsValue("-100000"),
		//		"max":       domains.ParamsValue("10.1241"),
		//		"precision": domains.ParamsValue("3"),
		//		"useType":   domains.ParamsValue("float4"),
		//	},
		//	result: result{
		//		pattern: `^-*\d+[.]*\d{0,3}$`,
		//	},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = domains.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			transformer, warnings, err := RandomFloatTransformerDefinition.Instance(
				context.Background(),
				driver,
				tt.params,
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
				resValue := val.Value.(*float64)
				assert.GreaterOrEqual(t, *resValue, tt.result.min)
				assert.LessOrEqual(t, *resValue, tt.result.max)
			}
		})
	}
}
