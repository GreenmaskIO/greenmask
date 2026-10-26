// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformers

import (
	"math"
	"strconv"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomIntTransformer_int8_default_range(t *testing.T) {
	values := parseFloats(t, transformRows(t, "RandomInt", "id8", rawValues("id8", "1"), paramValues(), nil, 20))
	var outOfInt16 bool
	for _, v := range values {
		outOfInt16 = outOfInt16 || math.Abs(v) > math.MaxInt16
	}
	require.True(t, outOfInt16, "int8 values are limited by int16 range: %v", values)
}

func TestRandomIntTransformer_limits_out_of_type_range(t *testing.T) {
	_, _, warns := newTestTransformer(t, "RandomInt", "id2", rawValues("id2", "1"),
		paramValues("min", "100000", "max", "200000"), nil)
	require.Len(t, warns, 2)
	require.Equal(t, "min", warns[0].Meta["ParameterName"])
	require.Equal(t, int64(100000), warns[0].Meta["ParameterValue"])
	require.Equal(t, "max", warns[1].Meta["ParameterName"])
	require.Equal(t, int64(200000), warns[1].Meta["ParameterValue"])

	_, _, warns = newTestTransformer(t, "RandomInt", "id2", rawValues("id2", "1"),
		paramValues("min", "-100", "max", "100"), nil)
	require.Empty(t, warns)
}

func TestRandomIntTransformer_inclusive_range(t *testing.T) {
	tests := []struct {
		name     string
		min      string
		max      string
		expected []float64
	}{
		{name: "negative", min: "-5", max: "-1", expected: []float64{-5, -4, -3, -2, -1}},
		{name: "positive", min: "1", max: "3", expected: []float64{1, 2, 3}},
		{name: "min equal max", min: "5", max: "5", expected: []float64{5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := parseFloats(t, transformRows(t, "RandomInt", "id4", rawValues("id4", "1"),
				paramValues("min", tt.min, "max", tt.max), nil, 200))
			requireAllInRange(t, values, tt.expected[0], tt.expected[len(tt.expected)-1])
			for _, e := range tt.expected {
				require.Contains(t, values, e)
			}
		})
	}
}

func TestRandomIntTransformer_dynamic_limits(t *testing.T) {
	t.Run("zero limits are not ignored", func(t *testing.T) {
		values := transformRows(t, "RandomInt", "id8",
			rawValues("id8", "1", "int8_val", "0", "int4_val", "0"), paramValues(),
			map[string]*toolkit.DynamicParamValue{"min": {Column: "int8_val"}, "max": {Column: "int4_val"}}, 10)
		for _, v := range values {
			require.Equal(t, "0", v)
		}
	})
	t.Run("dynamic min without max", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "RandomInt", "id8",
			rawValues("id8", "1", "int8_val", "10"), paramValues(),
			map[string]*toolkit.DynamicParamValue{"min": {Column: "int8_val"}}, 10))
		requireAllInRange(t, values, 10, math.MaxInt64)
	})
}

func TestRandomFloatTransformer_limits_out_of_type_range(t *testing.T) {
	_, _, warns := newTestTransformer(t, "RandomFloat", "col_float4", rawValues("col_float4", "1"),
		paramValues("min", "-1e300", "max", "1e300"), nil)
	require.Len(t, warns, 2)
	require.Equal(t, "min", warns[0].Meta["ParameterName"])
	require.Equal(t, "max", warns[1].Meta["ParameterName"])
	require.Equal(t, 1e300, warns[1].Meta["ParameterValue"])
}

func TestRandomFloatTransformer_wide_range(t *testing.T) {
	tests := []struct {
		name   string
		column string
		params map[string]toolkit.ParamsValue
		min    float64
		max    float64
	}{
		{name: "float8 default", column: "col_float8", params: paramValues("min", "0", "max", "0"), min: -math.MaxFloat64, max: math.MaxFloat64},
		{name: "float4 default", column: "col_float4", params: paramValues("min", "0", "max", "0"), min: -math.MaxFloat32, max: math.MaxFloat32},
		{name: "float8 big range", column: "col_float8", params: paramValues("min", "0", "max", "1e16"), min: 0, max: 1e16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := parseFloats(t, transformRows(t, "RandomFloat", tt.column, rawValues(tt.column, "1"), tt.params, nil, 10))
			requireAllInRange(t, values, tt.min, tt.max)
			unique := make(map[float64]struct{})
			for _, v := range values {
				unique[v] = struct{}{}
			}
			require.Greater(t, len(unique), 1, "values are not random: %v", values)
		})
	}
}

func TestRandomFloatTransformer_dynamic_zero_limits(t *testing.T) {
	values := parseFloats(t, transformRows(t, "RandomFloat", "col_float8",
		rawValues("col_float8", "1", "col_float4", "0"), paramValues("max", "0"),
		map[string]*toolkit.DynamicParamValue{"min": {Column: "col_float4"}}, 10))
	requireAllInRange(t, values, 0, 0)
}

func TestRandomNumericTransformer_decimal_range(t *testing.T) {
	values := transformRows(t, "RandomNumeric", "id_numeric", rawValues("id_numeric", "1"),
		paramValues("min", "0.5", "max", "0.9", "decimal", "2"), nil, 200)
	unique := make(map[string]struct{})
	for _, v := range values {
		d := decimal.RequireFromString(v)
		require.True(t, d.GreaterThanOrEqual(decimal.RequireFromString("0.5")) && d.LessThanOrEqual(decimal.RequireFromString("0.9")), v)
		unique[v] = struct{}{}
	}
	require.Greater(t, len(unique), 10)
}

func TestRandomNumericTransformer_dynamic_zero_limits(t *testing.T) {
	values := transformRows(t, "RandomNumeric", "id_numeric",
		rawValues("id_numeric", "1", "val_numeric", "0"), paramValues("max", "0"),
		map[string]*toolkit.DynamicParamValue{"min": {Column: "val_numeric"}}, 10)
	for _, v := range values {
		require.True(t, decimal.RequireFromString(v).IsZero(), v)
	}
}

func TestNoiseTransformers_min_ratio_greater_than_max_ratio(t *testing.T) {
	tests := []struct {
		name   string
		column string
	}{
		{name: "NoiseInt", column: "id4"},
		{name: "NoiseFloat", column: "col_float8"},
		{name: "NoiseNumeric", column: "id_numeric"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, warns := newTestTransformer(t, tt.name, tt.column, rawValues(tt.column, "1000"),
				paramValues("min_ratio", "0.9", "max_ratio", "0.1"), nil)
			require.Len(t, warns, 1)
			require.Equal(t, toolkit.ErrorValidationSeverity, warns[0].Severity)

			_, _, warns = newTestTransformer(t, tt.name, tt.column, rawValues(tt.column, "1000"),
				paramValues("min_ratio", "0.1", "max_ratio", "0.1"), nil)
			require.Empty(t, warns)
		})
	}
}

func TestNoiseIntTransformer_dynamic_limits(t *testing.T) {
	t.Run("dynamic max", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseInt", "id8", rawValues("id8", "1000", "int8_val", "50"),
			paramValues(), map[string]*toolkit.DynamicParamValue{"max": {Column: "int8_val"}}, 10))
		requireAllInRange(t, values, math.MinInt64, 50)
	})
	t.Run("dynamic min", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseInt", "id8", rawValues("id8", "1000", "int8_val", "2000"),
			paramValues(), map[string]*toolkit.DynamicParamValue{"min": {Column: "int8_val"}}, 10))
		requireAllInRange(t, values, 2000, math.MaxInt64)
	})
	t.Run("dynamic min and static max", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseInt", "id8", rawValues("id8", "1000", "int8_val", "10"),
			paramValues("max", "500"), map[string]*toolkit.DynamicParamValue{"min": {Column: "int8_val"}}, 10))
		requireAllInRange(t, values, 10, 500)
	})
}

func TestNoiseIntTransformer_no_overflow(t *testing.T) {
	values := transformRows(t, "NoiseInt", "id8", rawValues("id8", "8000000000000000000"),
		paramValues("min_ratio", "0.2", "max_ratio", "0.3"), nil, 20)
	for _, v := range values {
		i, err := strconv.ParseInt(v, 10, 64)
		require.NoError(t, err)
		require.GreaterOrEqual(t, i, int64(5000000000000000000), "value wrapped around: %d", i)
	}
}

func TestNoiseFloatTransformer_dynamic_limits(t *testing.T) {
	t.Run("dynamic max", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseFloat", "col_float8",
			rawValues("col_float8", "100", "col_float4", "1000"), paramValues(),
			map[string]*toolkit.DynamicParamValue{"max": {Column: "col_float4"}}, 10))
		requireAllInRange(t, values, 79.9, 120.1)
	})
	t.Run("dynamic min and static max", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseFloat", "col_float8",
			rawValues("col_float8", "500", "col_float4", "10"), paramValues("max", "300"),
			map[string]*toolkit.DynamicParamValue{"min": {Column: "col_float4"}}, 10))
		requireAllInRange(t, values, 300, 300)
	})
}

func TestNoiseFloatTransformer_clamped_value_rounded(t *testing.T) {
	values := transformRows(t, "NoiseFloat", "col_float8", rawValues("col_float8", "500"),
		paramValues("decimal", "1", "max", "300.123"), nil, 5)
	for _, v := range values {
		require.Equal(t, "300.1", v)
	}
}

func TestNoiseNumericTransformer_dynamic_limits(t *testing.T) {
	t.Run("dynamic min without max", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseNumeric", "id_numeric",
			rawValues("id_numeric", "100", "val_numeric", "50"), paramValues(),
			map[string]*toolkit.DynamicParamValue{"min": {Column: "val_numeric"}}, 10))
		requireAllInRange(t, values, 79.9, 120.1)
	})
	t.Run("dynamic max without min keeps negative values", func(t *testing.T) {
		values := parseFloats(t, transformRows(t, "NoiseNumeric", "id_numeric",
			rawValues("id_numeric", "-100", "val_numeric", "-50"), paramValues(),
			map[string]*toolkit.DynamicParamValue{"max": {Column: "val_numeric"}}, 10))
		requireAllInRange(t, values, -120.1, -79.9)
	})
}

func TestNoiseNumericTransformer_clamped_value_rounded(t *testing.T) {
	values := transformRows(t, "NoiseNumeric", "id_numeric", rawValues("id_numeric", "200.456"),
		paramValues("decimal", "0", "max", "100.55"), nil, 5)
	for _, v := range values {
		require.Equal(t, "100", v)
	}
}
