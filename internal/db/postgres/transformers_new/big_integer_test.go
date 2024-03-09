package transformers_new

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestBigIntTransformer_Transform_random_static(t *testing.T) {

	type expected struct {
		min    decimal.Decimal
		max    decimal.Decimal
		isNull bool
	}

	tests := []struct {
		name          string
		columnName    string
		originalValue string
		params        map[string]toolkit.ParamsValue
		expected      expected
	}{
		{
			name:          "numeric",
			columnName:    "id_numeric",
			originalValue: "12345",
			params: map[string]toolkit.ParamsValue{
				"min": toolkit.ParamsValue("10000000000000000000"),
				"max": toolkit.ParamsValue("100000000000000000000"),
			},
			expected: expected{
				min: decimal.RequireFromString("10000000000000000000"),
				max: decimal.RequireFromString("100000000000000000000"),
			},
		},
		{
			name:          "keep_null false and NULL seq",
			columnName:    "id_numeric",
			originalValue: "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("10000000000000000000"),
				"max":       toolkit.ParamsValue("100000000000000000000"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			expected: expected{
				min: decimal.RequireFromString("10000000000000000000"),
				max: decimal.RequireFromString("100000000000000000000"),
			},
		},
		{
			name:          "keep_null true and NULL seq",
			columnName:    "id_numeric",
			originalValue: "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("10000000000000000000"),
				"max":       toolkit.ParamsValue("100000000000000000000"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			expected: expected{
				isNull: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.originalValue)
			def, ok := utils.DefaultTransformerRegistry.Get("random.BigInteger")
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

			var res decimal.Decimal
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)

			if tt.expected.isNull {
				require.True(t, empty)
			} else {
				require.True(t, res.GreaterThanOrEqual(tt.expected.min) && res.LessThanOrEqual(tt.expected.max))
			}
		})
	}

}

func TestBigIntTransformer_Transform_random_dynamic(t *testing.T) {

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
			name:       "id_numeric",
			columnName: "id_numeric",
			record: map[string]*toolkit.RawValue{
				"id_numeric":  toolkit.NewRawValue([]byte("-1000020102102"), false),
				"val_numeric": toolkit.NewRawValue([]byte("10"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max": toolkit.ParamsValue("10000000"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "val_numeric",
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
			def, ok := utils.DefaultTransformerRegistry.Get("random.BigInteger")
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

			var res int64
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}

func TestBigIntTransformer_Transform_deterministic_dynamic(t *testing.T) {
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
			name:       "numeric",
			columnName: "id_numeric",
			record: map[string]*toolkit.RawValue{
				"id_numeric":  toolkit.NewRawValue([]byte("-1000020102102"), false),
				"val_numeric": toolkit.NewRawValue([]byte("10"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max":  toolkit.ParamsValue("10000000"),
				"salt": toolkit.ParamsValue("12345abcd"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "val_numeric",
				},
			},
			expected: expected{
				min: 10,
				max: 10000000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			driver, record := toolkit.GetDriverAndRecord(tt.record)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			def, ok := utils.DefaultTransformerRegistry.Get("deterministic.BigInteger")
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

			var res int64
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}
