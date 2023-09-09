package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		params     map[string][]byte
		pattern    string
	}{
		{
			name:       "default fixed string",
			columnName: "data",
			params: map[string][]byte{
				"minLength": []byte("10"),
				"maxLength": []byte("10"),
			},
			pattern: `^\w{10}$`,
		},
		{
			name:       "default variadic string",
			columnName: "data",
			params: map[string][]byte{
				"minLength": []byte("2"),
				"maxLength": []byte("30"),
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name:       "custom variadic string",
			columnName: "data",
			params: map[string][]byte{
				"minLength": []byte("10"),
				"maxLength": []byte("10"),
				"symbols":   []byte("1234567890"),
			},
			pattern: `^\d{10}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, "some")
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
