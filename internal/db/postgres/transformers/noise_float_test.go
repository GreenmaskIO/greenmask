package transformers

import (
	"context"
	"log"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoiseFloatTransformer_Transform(t *testing.T) {

	type result struct {
		min    float64
		max    float64
		regexp string
	}

	tests := []struct {
		name       string
		columnName string
		params     map[string][]byte
		input      string
		result     result
	}{
		{
			name:       "float4",
			columnName: "col_float4",
			params: map[string][]byte{
				"ratio": []byte("0.9"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `-*\d+[.]*\d*$`},
		},
		{
			name:       "float8",
			columnName: "col_float8",
			params: map[string][]byte{
				"ratio": []byte("0.9"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `-*\d+[.]*\d*$`},
		},
		{
			name:       "float8 ranges 1",
			columnName: "col_float8",
			params: map[string][]byte{
				"ratio":     []byte("0.9"),
				"precision": []byte("10"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+[.]*\d{0,10}$`},
		},
		{
			name:       "float8 ranges 1 with precision",
			columnName: "col_float8",
			params: map[string][]byte{
				"ratio":     []byte("0.9"),
				"precision": []byte("0"),
			},
			input:  "100",
			result: result{min: 10, max: 190, regexp: `^-*\d+$`},
		},
		//{
		//	name: "text with default float8",
		//	params: map[string][]byte{
		//		"ratio":     0.9,
		//		"precision": 3,
		//		"useType":   "float4",
		//		"column":    "test",
		//	},
		//	input:   "100",
		//	result:  result{min: 10, max: 190},
		//	regexp: `^-*\d+[.]*\d{0,3}$`,
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.input)
			transformer, warnings, err := NoiseFloatTransformerDefinition.Instance(
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
			res, err := r.EncodeAttr(tt.columnName)
			log.Println(string(res))
			require.Regexp(t, tt.result.regexp, string(res))
			resFloat, err := strconv.ParseFloat(string(res), 64)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, resFloat, tt.result.min)
			assert.LessOrEqual(t, resFloat, tt.result.max)
		})
	}
}
