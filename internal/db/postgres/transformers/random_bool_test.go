package transformers

import (
	"context"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {

	tests := []struct {
		name       string
		params     map[string]toolkit.ParamsValue
		columnName string
		original   string
		isNull     bool
	}{
		{
			name:       "common",
			original:   "t",
			columnName: "col_bool",
			params:     map[string]toolkit.ParamsValue{},
		},
		{
			name:       "keep_null false and NULL seq",
			original:   "\\N",
			columnName: "col_bool",
			params: map[string]toolkit.ParamsValue{
				"keep_null": toolkit.ParamsValue("false"),
			},
		},
		{
			name:       "keep_null true and NULL seq",
			original:   "\\N",
			columnName: "col_bool",
			params: map[string]toolkit.ParamsValue{
				"keep_null": toolkit.ParamsValue("true"),
			},
			isNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			transformer, warnings, err := RandomBoolTransformerDefinition.Instance(
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
			require.Equal(t, tt.isNull, val.IsNull)
			if !tt.isNull {
				assert.IsType(t, val.Value, true)
			}
		})
	}
}
