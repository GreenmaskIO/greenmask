package transformers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

func TestRandomFloatTransformer_Transform(t *testing.T) {
	type result struct {
		min        float64
		max        float64
		pattern    string
		checkRange bool
	}

	tests := []struct {
		name          string
		params        map[string][]byte
		columnName    string
		originalValue string
		result        result
	}{
		{
			name:          "float4",
			columnName:    "col_float4",
			originalValue: "1000.0",
			params: map[string][]byte{
				"min": []byte("1"),
				"max": []byte("10"),
			},
			result: result{
				min:        1,
				max:        10,
				pattern:    `-*\d+[.]*\d*$`,
				checkRange: true,
			},
		},
		{
			name:          "float8",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string][]byte{
				"min": []byte("1"),
				"max": []byte("10"),
			},
			result: result{
				min:        1,
				max:        10,
				pattern:    `-*\d+[.]*\d*$`,
				checkRange: true,
			},
		},
		{
			name:          "float8 ranges 1",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string][]byte{
				"min":       []byte("-100000"),
				"max":       []byte("100000"),
				"precision": []byte("10"),
			},
			result: result{
				min:        -100000,
				max:        100000,
				pattern:    `^-*\d+[.]*\d{0,10}$`,
				checkRange: true,
			},
		},
		{
			name:          "float8 ranges 1 with precision",
			columnName:    "col_float8",
			originalValue: "1000.0",
			params: map[string][]byte{
				"min":       []byte("-100000"),
				"max":       []byte("-1"),
				"precision": []byte("0"),
			},
			result: result{
				min:        -100000,
				max:        -1,
				pattern:    `^-\d+$`,
				checkRange: true,
			},
		},
		{
			name:          "keep_null false and NULL seq",
			columnName:    "col_float8",
			originalValue: toolkit.DefaultNullSeq,
			params: map[string][]byte{
				"min":       []byte("-100000"),
				"max":       []byte("-1"),
				"precision": []byte("0"),
				"keep_null": []byte("false"),
			},
			result: result{
				min:        -100000,
				max:        -1,
				pattern:    `^-\d+$`,
				checkRange: true,
			},
		},
		{
			name:          "keep_null true and NULL seq",
			columnName:    "col_float8",
			originalValue: toolkit.DefaultNullSeq,
			params: map[string][]byte{
				"min":       []byte("-100000"),
				"max":       []byte("-1"),
				"precision": []byte("0"),
				"keep_null": []byte("true"),
			},
			result: result{
				min:        -100000,
				max:        -1,
				pattern:    fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
				checkRange: false,
			},
		},
		//{
		//	name: "text with default float8",
		//	params: map[string][]byte{
		//		"min":       []byte("-100000"),
		//		"max":       []byte("10.1241"),
		//		"precision": []byte("3"),
		//		"useType":   []byte("float4"),
		//	},
		//	result: result{
		//		pattern: `^-*\d+[.]*\d{0,3}$`,
		//	},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
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
			res, err := r.EncodeAttr(tt.columnName)
			require.NoError(t, err)
			log.Println(res)
			require.Regexp(t, tt.result.pattern, string(res))
			if tt.result.checkRange {
				resFloat, err := strconv.ParseFloat(string(res), 64)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, resFloat, tt.result.min)
				assert.LessOrEqual(t, resFloat, tt.result.max)
			}
		})
	}
}
