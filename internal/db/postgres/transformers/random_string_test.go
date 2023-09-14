package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string][]byte
		pattern    string
	}{
		{
			name:       "default fixed string",
			original:   "some",
			columnName: "data",
			params: map[string][]byte{
				"min_length": []byte("10"),
				"max_length": []byte("10"),
			},
			pattern: `^\w{10}$`,
		},
		{
			name:       "default variadic string",
			original:   "some",
			columnName: "data",
			params: map[string][]byte{
				"min_length": []byte("2"),
				"max_length": []byte("30"),
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name:       "custom variadic string",
			original:   "some",
			columnName: "data",
			params: map[string][]byte{
				"min_length": []byte("10"),
				"max_length": []byte("10"),
				"symbols":    []byte("1234567890"),
			},
			pattern: `^\d{10}$`,
		},
		{
			name:       "keep_null",
			original:   toolkit.DefaultNullSeq,
			columnName: "data",
			params: map[string][]byte{
				"min_length": []byte("10"),
				"max_length": []byte("10"),
				"symbols":    []byte("1234567890"),
				"keep_null":  []byte("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, toolkit.DefaultNullSeq),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RandomStringTransformerDefinition.Instance(
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
			require.Regexp(t, tt.pattern, string(res))
		})
	}
}
