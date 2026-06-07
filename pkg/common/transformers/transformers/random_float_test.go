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
	"regexp"
	"strconv"
	"testing"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomFloatTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]core.ParamsValue
		dynamicParameter map[string]core.DynamicParamValue
		original         []*core.ColumnRawValue
		validateFn       func(t *testing.T, recorder core.Recorder)
		expectedErr      string
		columns          []core.Column
	}{
		{
			name: "float4",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("1000.0"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"min":    core.ParamsValue("1"),
				"max":    core.ParamsValue("10"),
				"engine": core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, 1.0)
				assert.LessOrEqual(t, val, 10.0)

			},
		},
		{
			name: "keep_null false and NULL seq",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"min":       core.ParamsValue("-10000"),
				"max":       core.ParamsValue("10000"),
				"keep_null": core.ParamsValue("false"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var expectedMin = -10000.0
				var expectedMax = 10000.0
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val >= expectedMin && val <= expectedMax)
			},
		},
		{
			name: "keep_null true and NULL seq",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"min":       core.ParamsValue("10000000000000000000"),
				"max":       core.ParamsValue("100000000000000000000"),
				"keep_null": core.ParamsValue("true"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "decimals",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"min":       core.ParamsValue("-10000"),
				"max":       core.ParamsValue("10000"),
				"keep_null": core.ParamsValue("false"),
				"decimal":   core.ParamsValue("2"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var expectedMin = -10000.0
				var expectedMax = 10000.0
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val >= expectedMin && val <= expectedMax)
				valStr := strconv.FormatFloat(val, 'f', -1, 64)
				matched, err := regexp.MatchString(`^-*\d+[.]*\d{0,2}$`, valStr)
				require.NoError(t, err)
				require.True(t, matched, "value %s does not match regexp for max 2 decimal places", valStr)
			},
		},
		{
			name: "Dynamic mode",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"keep_null": core.ParamsValue("false"),
				"decimal":   core.ParamsValue("2"),
			},
			dynamicParameter: map[string]core.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("1234"), false),
				core.NewColumnRawValue([]byte("-10"), false),
				core.NewColumnRawValue([]byte("10"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  mysqldbmsdriver.TypeFloat,
					TypeOID:   mysqldbmsdriver.VirtualOidFloat,
					TypeClass: core.TypeClassFloat,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var expectedMin = -10.0
				var expectedMax = 10.0
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val >= expectedMin && val <= expectedMax)
				valStr := strconv.FormatFloat(val, 'f', -1, 64)
				matched, err := regexp.MatchString(`^-*\d+[.]*\d{0,2}$`, valStr)
				require.NoError(t, err)
				require.True(t, matched, "value %s does not match regexp for max 2 decimal places", valStr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RamdomFloatTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, nil, true))
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}
			tt.validateFn(t, env.GetRecord())
		})
	}
}
