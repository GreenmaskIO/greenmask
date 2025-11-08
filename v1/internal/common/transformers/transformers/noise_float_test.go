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
	"context"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestNoiseFloatTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		original         []*commonmodels.ColumnRawValue
		validateFn       func(t *testing.T, recorder commonininterfaces.Recorder)
		expectedErr      string
		columns          []commonmodels.Column
		isNull           bool
	}{
		{
			name: "float4",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Length:    0,
					Size:      4,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// Need to check that the value is in between
				// [current - 0.9*current, current - 0.2*current] U [current + 0.2*current, current + 0.9*current]
				// i.e. [10, 80] U [120, 190]
				require.True(t, (value >= 10 && value <= 80) || (value >= 120 && value <= 190),
					"expected value to be in between [10, 80] U [120, 190], got %f", value)
			},
		},
		{
			name: "float8",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// Need to check that the value is in between
				// [current - 0.9*current, current - 0.2*current] U [current + 0.2*current, current + 0.9*current]
				// i.e. [10, 80] U [120, 190]
				require.True(t, (value >= 10 && value <= 80) || (value >= 120 && value <= 190),
					"expected value to be in between [10, 80] U [120, 190], got %f", value)
			},
		},
		{
			name: "10 decimals",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"decimal":   commonmodels.ParamsValue("10"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				parts := strings.Split(string(val.Data), ".")
				require.Len(t, parts, 2, "expected float with decimal point")
				require.LessOrEqual(t, len(parts[1]), 10,
					"expected decimal part to be less than or equal to 10 digits")
			},
		},
		{
			name: "0 decimals",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"decimal":   commonmodels.ParamsValue("0"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				parts := strings.Split(string(val.Data), ".")
				require.Len(t, parts, 1, "expected float without decimal point")
			},
		},
		{
			name: "deterministic engine",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"decimal":   commonmodels.ParamsValue("0"),
				"engine":    commonmodels.ParamsValue("deterministic"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				// The value must be alway the same!
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				require.Equal(t, 138.0, value)
			},
		},
		{
			name: "with thresholds",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"min":       commonmodels.ParamsValue("90"),
				"max":       commonmodels.ParamsValue("110"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between 90 and 110 and in the min and max ratio range
				require.GreaterOrEqual(t, value, 90.0)
				require.LessOrEqual(t, value, 110.0)
			},
		},
		{
			name: "with thresholds",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"min":       commonmodels.ParamsValue("0"),
				"max":       commonmodels.ParamsValue("110"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between 0 and 110 and in the min and max ratio range
				require.GreaterOrEqual(t, value, 0.0)
				require.LessOrEqual(t, value, 110.0)
			},
		},
		{
			name: "dynamic mode with min and max",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"min": {
					Column: "min_col",
				},
				"max": {
					Column: "max_col",
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100000.2"), false),
				commonmodels.NewColumnRawValue([]byte("0"), false),
				commonmodels.NewColumnRawValue([]byte("110"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_col",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_col",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between 0 and 110 and in the min and max ratio range
				require.GreaterOrEqual(t, value, 0.0)
				require.LessOrEqual(t, value, 110.0)
			},
		},
		{
			name: "dynamic mode with min and empty max",
			// This should use the default max value for float8.
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"min": {
					Column: "min_col",
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("-100000.2"), false),
				commonmodels.NewColumnRawValue([]byte("0"), false),
				commonmodels.NewColumnRawValue([]byte("110"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_col",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_col",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between 0 and 110 and in the min and max ratio range
				require.GreaterOrEqual(t, value, 0.0)
				require.LessOrEqual(t, value, math.MaxFloat64)
			},
		},
		{
			name: "dynamic mode with max and empty min",
			// This should use the default max value for float8.
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"max": {
					Column: "max_col",
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("-100000.2"), false),
				commonmodels.NewColumnRawValue([]byte("0"), false),
				commonmodels.NewColumnRawValue([]byte("110"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_col",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_col",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: commonmodels.TypeClassFloat,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				var value float64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between -MaxFloat64 and 110 and in the min and max ratio range
				require.GreaterOrEqual(t, value, -math.MaxFloat64)
				require.LessOrEqual(t, value, 110.0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				NoiseFloatTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			tt.validateFn(t, env.GetRecord())
		})
	}
}
