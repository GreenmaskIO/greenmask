package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	utils2 "github.com/greenmaskio/greenmask/internal/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomIntTransformer_Transform_random_static(t *testing.T) {

	type expected struct {
		min    int64
		max    int64
		isNull bool
	}

	tests := []struct {
		name           string
		columnName     string
		originalValue  string
		expectedRegexp string
		params         map[string]toolkit.ParamsValue
		expected       expected
	}{
		{
			name:           "int2",
			columnName:     "id2",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("100"),
			},
			expected: expected{
				min: 1,
				max: 100,
			},
		},
		{
			name:           "int4",
			columnName:     "id4",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("100"),
			},
			expected: expected{
				min: 1,
				max: 100,
			},
		},
		{
			name:           "int8",
			columnName:     "id8",
			originalValue:  "12345",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("1"),
				"max": toolkit.ParamsValue("100"),
			},
			expected: expected{
				min: 1,
				max: 100,
			},
		},
		{
			name:           "keep_null false and NULL seq",
			columnName:     "id8",
			originalValue:  "\\N",
			expectedRegexp: `^\d{1,3}$`,
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1"),
				"max":       toolkit.ParamsValue("100"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			expected: expected{
				min: 1,
				max: 100,
			},
		},
		{
			name:           "keep_null true and NULL seq",
			columnName:     "id8",
			originalValue:  "\\N",
			expectedRegexp: fmt.Sprintf(`^(\%s)$`, "\\N"),
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1"),
				"max":       toolkit.ParamsValue("100"),
				"keep_null": toolkit.ParamsValue("true"),
				"engine":    toolkit.ParamsValue("random"),
			},
			expected: expected{
				min:    1,
				max:    100,
				isNull: true,
			},
		},
		{
			name:           "test zero min",
			columnName:     "id8",
			originalValue:  "\\N",
			expectedRegexp: fmt.Sprintf(`^(\%s)$`, "\\N"),
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("0"),
				"max":       toolkit.ParamsValue("100"),
				"engine":    toolkit.ParamsValue("random"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			expected: expected{
				min: 0,
				max: 100,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomInt")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)
			rawData, _ := r.GetRawColumnValueByName(tt.columnName)
			require.Equal(t, tt.expected.isNull, rawData.IsNull)
			if !rawData.IsNull {
				var resInt int64
				_, err = r.ScanColumnValueByName(tt.columnName, &resInt)
				require.NoError(t, err)
				require.True(t, resInt >= tt.expected.min && resInt <= tt.expected.max)
			}
		})
	}

}

func TestRandomIntTransformer_Transform_random_dynamic(t *testing.T) {

	type expected struct {
		min int64
		max int64
	}

	tests := []struct {
		name          string
		columnName    string
		params        map[string]toolkit.ParamsValue
		dynamicParams map[string]*toolkit.DynamicParamValue
		record        map[string]*toolkit.RawValue
		expected      expected
	}{
		{
			name:       "int4",
			columnName: "id4",
			record: map[string]*toolkit.RawValue{
				"id4":      toolkit.NewRawValue([]byte("123"), false),
				"int4_val": toolkit.NewRawValue([]byte("10"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max":    toolkit.ParamsValue("10000000"),
				"engine": toolkit.ParamsValue("random"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "int4_val",
				},
			},
			expected: expected{
				min: 123,
				max: 10000000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			driver, record := toolkit.GetDriverAndRecord(tt.record)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomInt")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				tt.dynamicParams,
				"",
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

			var res int64
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}

func TestRandomIntTransformer_Transform_deterministic_dynamic(t *testing.T) {
	type expected struct {
		min int64
		max int64
	}

	tests := []struct {
		name          string
		columnName    string
		params        map[string]toolkit.ParamsValue
		dynamicParams map[string]*toolkit.DynamicParamValue
		record        map[string]*toolkit.RawValue
		expected      expected
	}{
		{
			name:       "int4",
			columnName: "id4",
			record: map[string]*toolkit.RawValue{
				"id4":      toolkit.NewRawValue([]byte("123"), false),
				"int4_val": toolkit.NewRawValue([]byte("10"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max":    toolkit.ParamsValue("10000000"),
				"engine": toolkit.ParamsValue("hash"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "int4_val",
				},
			},
			expected: expected{
				min: 10,
				max: 10000000,
			},
		},
	}

	ctx := utils2.WithSalt(context.Background(), []byte("ffaacac"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			driver, record := toolkit.GetDriverAndRecord(tt.record)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomInt")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				ctx,
				driver,
				tt.params,
				tt.dynamicParams,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			err = transformer.Transformer.Init(ctx)
			require.NoError(t, err)

			for _, dp := range transformer.DynamicParameters {
				dp.SetRecord(record)
			}

			r, err := transformer.Transformer.Transform(
				ctx,
				record,
			)
			require.NoError(t, err)

			var res int64
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}
