package transformers_new

import (
	"context"
	"fmt"
	"testing"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestRandomIntTransformer_Transform_random_static(t *testing.T) {

	tests := []struct {
		name           string
		columnName     string
		originalValue  string
		expectedRegexp string
		params         map[string]toolkit.ParamsValue
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			def, ok := utils.DefaultTransformerRegistry.Get("random.Integer")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				nil,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			encoded, err := r.Encode()
			require.NoError(t, err)
			res, err := encoded.Encode()
			require.NoError(t, err)
			require.Regexp(t, tt.expectedRegexp, string(res))
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
			name:       "int2",
			columnName: "id",
			record: map[string]*toolkit.RawValue{
				"id":       toolkit.NewRawValue([]byte("123"), false),
				"int_val2": toolkit.NewRawValue([]byte("10"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max": toolkit.ParamsValue("10000000"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "int_val2",
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
			def, ok := utils.DefaultTransformerRegistry.Get("random.Integer")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				tt.dynamicParams,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			for _, dp := range transformer.DynamicParameters {
				dp.SetRecord(record)
			}

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res int64
			empty, err := r.ScanColumnValueByName("id", &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}

func TestRandomIntTransformer_Transform_random_deterministic(t *testing.T) {
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
			name:       "int2",
			columnName: "id",
			record: map[string]*toolkit.RawValue{
				"id":       toolkit.NewRawValue([]byte("123"), false),
				"int_val2": toolkit.NewRawValue([]byte("10"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max":           toolkit.ParamsValue("10000000"),
				"salt":          toolkit.ParamsValue("12345abcd"),
				"hash_function": toolkit.ParamsValue("sha1"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "int_val2",
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
			def, ok := utils.DefaultTransformerRegistry.Get("deterministic.Integer")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				tt.dynamicParams,
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			for _, dp := range transformer.DynamicParameters {
				dp.SetRecord(record)
			}

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res int64
			empty, err := r.ScanColumnValueByName("id", &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}
