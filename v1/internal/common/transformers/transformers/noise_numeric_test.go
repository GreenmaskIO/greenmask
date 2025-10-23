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

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestNoiseNumericTransformer_Transform(t *testing.T) {
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
			name: "numeric",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("random"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("random"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"decimal":   commonmodels.ParamsValue("10"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("random"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"decimal":   commonmodels.ParamsValue("0"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"decimal":   commonmodels.ParamsValue("0"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				val, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				require.Equal(t, "162", val.String())
			},
		},
		{
			name: "numeric with thresholds",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("random"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
				"min":       commonmodels.ParamsValue("90"),
				"max":       commonmodels.ParamsValue("110"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("random"),
				"min_ratio": commonmodels.ParamsValue("0.2"),
				"max_ratio": commonmodels.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("100"), false),
				commonmodels.NewColumnRawValue([]byte("90"), false),
				commonmodels.NewColumnRawValue([]byte("100"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
				{
					Idx:      1,
					Name:     "min_val",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
				{
					Idx:      2,
					Name:     "max_val",
					TypeName: mysqldbmsdriver.TypeNumeric,
					TypeOID:  mysqldbmsdriver.VirtualOidNumeric,
					Length:   0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
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
