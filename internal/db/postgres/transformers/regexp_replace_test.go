package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexpReplaceTransformer_Transform2(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string][]byte
		columnName string
		original   string
		expected   string
	}{
		{
			name: "common",
			params: map[string][]byte{
				"regexp":  []byte(`(Hello)\s*world\s*(\!+\?)`),
				"replace": []byte("$1 Mr NoName $2"),
			},
			columnName: "data",
			original:   "Hello world!!!?",
			expected:   "Hello Mr NoName !!!?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = []byte(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RegexpReplaceTransformerDefinition.Instance(
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
			require.Equal(t, tt.expected, string(res))
		})
	}

}
