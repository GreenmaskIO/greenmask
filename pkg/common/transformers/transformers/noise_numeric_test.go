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
	"testing"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	coretest "github.com/greenmaskio/greenmask/pkg/common/coretest"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestNoiseNumericTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]core.ParamsValue
		dynamicParameter map[string]core.DynamicParamValue
		original         []*core.ColumnRawValue
		validateFn       func(t *testing.T, recorder core.Recorder)
		expectedErr      string
		columns          []core.Column
		isNull           bool
	}{
		{
			name: "numeric",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("random"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				expectedMin := decimal.RequireFromString("10")
				expectedMax := decimal.RequireFromString("190")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "numeric with 10 decimal places",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("random"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
				"decimal":   core.ParamsValue("10"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				expectedMin := decimal.RequireFromString("10")
				expectedMax := decimal.RequireFromString("190")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
				// Validate that decimal has max 10 decimal places
				valStr := val.String()
				log.Info().Msgf("Transformed value: %s", valStr)
				matched, err := regexp.MatchString(`^-*\d+[.]*\d{0,10}$`, valStr)
				require.NoError(t, err)
				require.True(t, matched, "value %s does not match regexp for max 10 decimal places", valStr)
			},
		},
		{
			name: "numeric without decimal places",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("random"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
				"decimal":   core.ParamsValue("0"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				expectedMin := decimal.RequireFromString("10")
				expectedMax := decimal.RequireFromString("190")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
				// Validate that no decimal places
				valStr := val.String()
				log.Info().Msgf("Transformed value: %s", valStr)
				matched, err := regexp.MatchString(`^-*\d+$`, valStr)
				require.NoError(t, err)
				require.True(t, matched, "value %s does not match regexp for no decimal places", valStr)
			},
		},
		{
			name: "numeric with no decimal and deterministic engine",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
				"decimal":   core.ParamsValue("0"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				val, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				require.Equal(t, "162", val.String())
			},
		},
		{
			name: "numeric with thresholds",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("random"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
				"min":       core.ParamsValue("90"),
				"max":       core.ParamsValue("110"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				expectedMin := decimal.RequireFromString("90")
				expectedMax := decimal.RequireFromString("110")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "Dynamic mode",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("random"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
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
				core.NewColumnRawValue([]byte("100"), false),
				core.NewColumnRawValue([]byte("90"), false),
				core.NewColumnRawValue([]byte("100"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  coretest.TypeNumeric,
					TypeID:    coretest.TypeIDNumeric,
					TypeClass: core.TypeClassNumeric,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				expectedMin := decimal.RequireFromString("90")
				expectedMax := decimal.RequireFromString("110")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				NoiseNumericTransformerDefinition,
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
