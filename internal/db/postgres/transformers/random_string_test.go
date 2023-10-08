package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

// TODO: Cover error cases
func TestRandomStringTransformer_Transform(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string]toolkit.ParamsValue
		pattern    string
	}{
		{
			name:       "default fixed string",
			original:   "some",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("10"),
				"max_length": toolkit.ParamsValue("10"),
			},
			pattern: `^\w{10}$`,
		},
		{
			name:       "default variadic string",
			original:   "some",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("2"),
				"max_length": toolkit.ParamsValue("30"),
			},
			pattern: `^\w{2,30}$`,
		},
		{
			name:       "custom variadic string",
			original:   "some",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("10"),
				"max_length": toolkit.ParamsValue("10"),
				"symbols":    toolkit.ParamsValue("1234567890"),
			},
			pattern: `^\d{10}$`,
		},
		{
			name:       "keep_null",
			original:   "\\N",
			columnName: "data",
			params: map[string]toolkit.ParamsValue{
				"min_length": toolkit.ParamsValue("10"),
				"max_length": toolkit.ParamsValue("10"),
				"symbols":    toolkit.ParamsValue("1234567890"),
				"keep_null":  toolkit.ParamsValue("true"),
			},
			pattern: fmt.Sprintf(`^(\%s)$`, "\\N"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
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
			require.NoError(t, err)
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			require.Regexp(t, tt.pattern, string(res))
		})
	}
}
