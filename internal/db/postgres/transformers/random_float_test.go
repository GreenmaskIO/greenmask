package transformers

import (
	"context"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
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
		params        map[string]toolkit.ParamsValue
		columnName    string
		originalValue string
		result        result
	}{
		{
			name:          "float4",
			columnName:    "col_float4",
			originalValue: "1000.0",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("10"),
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
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("10"),
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
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("-100000"),
				"max":       toolkit.ParamsValue("100000"),
				"precision": toolkit.ParamsValue("10"),
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
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("-100000"),
				"max":       toolkit.ParamsValue("-1"),
				"precision": toolkit.ParamsValue("0"),
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
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("-100000"),
				"max":       toolkit.ParamsValue("-1"),
				"precision": toolkit.ParamsValue("0"),
				"keep_null": toolkit.ParamsValue("false"),
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
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("-100000"),
				"max":       toolkit.ParamsValue("-1"),
				"precision": toolkit.ParamsValue("0"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			result: result{
				isNull: true,
			},
		},
		//{
		//	name: "text with default float8",
		//	params: map[string]toolkit.ParamsValue{
		//		"min":       toolkit.ParamsValue("-100000"),
		//		"max":       toolkit.ParamsValue("10.1241"),
		//		"precision": toolkit.ParamsValue("3"),
		//		"useType":   toolkit.ParamsValue("float4"),
		//	},
		//	result: result{
		//		pattern: `^-*\d+[.]*\d{0,3}$`,
		//	},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
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

			val, err := r.GetAttributeByName(tt.columnName)
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
