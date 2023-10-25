package transformers

import (
	"context"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestRegexpReplaceTransformer_Transform2(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string]toolkit.ParamsValue
		columnName string
		original   string
		expected   string
	}{
		{
			name: "common",
			params: map[string]toolkit.ParamsValue{
				"regexp":  toolkit.ParamsValue(`(Hello)\s*world\s*(\!+\?)`),
				"replace": toolkit.ParamsValue("$1 Mr NoName $2"),
			},
			columnName: "data",
			original:   "Hello world!!!?",
			expected:   "Hello Mr NoName !!!?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
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
			var res string
			isNull, err := r.ScanAttributeByName(tt.columnName, &res)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, tt.expected, res)
		})
	}

}
