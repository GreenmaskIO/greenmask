package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUuidTransformer_Transform_uuid_type(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		params     map[string]toolkit.ParamsValue
		original   string
		regexp     string
	}{
		{
			name:       "text",
			columnName: "data",
			params:     map[string]toolkit.ParamsValue{},
			original:   "someval",
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "uuid",
			columnName: "uid",
			original:   "ddfb6f74-1771-45b0-b258-ae6fcd42f505",
			params:     map[string]toolkit.ParamsValue{},
			regexp:     `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull false and NULL seq",
			columnName: "uid",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"keep_null": toolkit.ParamsValue("false"),
			},
			regexp: `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`,
		},
		{
			name:       "keepNull true and NULL seq",
			columnName: "uid",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"keep_null": toolkit.ParamsValue("true"),
			},
			regexp: fmt.Sprintf(`^(\%s)$`, "\\N"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RandomUuidTransformerDefinition.Instance(
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
			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			assert.Regexp(t, tt.regexp, string(res))
		})
	}
}
