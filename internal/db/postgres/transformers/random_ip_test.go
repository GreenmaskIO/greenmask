package transformers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRandomIpTransformer_Transform_random_dynamic(t *testing.T) {

	tests := []struct {
		name          string
		columnName    string
		params        map[string]toolkit.ParamsValue
		dynamicParams map[string]*toolkit.DynamicParamValue
		record        map[string]*toolkit.RawValue
		expected      string
	}{
		{
			name:       "IPv4 dynamic test",
			columnName: "data",
			record: map[string]*toolkit.RawValue{
				"data":  toolkit.NewRawValue([]byte("192.168.1.10"), false),
				"data2": toolkit.NewRawValue([]byte("192.168.1.0/30"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"engine": toolkit.ParamsValue("random"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"subnet": {
					Column: "data2",
				},
			},
			expected: "192.168.1.[1,2]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			driver, record := toolkit.GetDriverAndRecord(tt.record)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomIp")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				tt.dynamicParams,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			err = transformer.Transformer.Init(context.Background())
			require.NoError(t, err)

			for _, dp := range transformer.DynamicParameters {
				dp.SetRecord(record)
			}

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			rawVal, err := r.GetRawColumnValueByName(tt.columnName)
			require.NoError(t, err)
			require.False(t, rawVal.IsNull)
			require.Regexp(t, tt.expected, string(rawVal.Data))
		})
	}
}
