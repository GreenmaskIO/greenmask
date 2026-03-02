// Copyright 2025 Greenmask
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

	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestRandomNumericTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]models.ParamsValue
		dynamicParameter map[string]models.DynamicParamValue
		original         []*models.ColumnRawValue
		validateFn       func(t *testing.T, recorder commonininterfaces.Recorder)
		expectedErr      string
		columns          []models.Column
	}{
		{
			name: "numeric",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue([]byte("1234567"), false)},
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"engine": models.ParamsValue("deterministic"),
				"min":    models.ParamsValue("10000000000000000000"),
				"max":    models.ParamsValue("100000000000000000000"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("10000000000000000000")
				expectedMax := decimal.RequireFromString("100000000000000000000")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "keep_null false and NULL seq",
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"min":       models.ParamsValue("10000000000000000000"),
				"max":       models.ParamsValue("100000000000000000000"),
				"keep_null": models.ParamsValue("false"),
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue(nil, true)},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("10000000000000000000")
				expectedMax := decimal.RequireFromString("100000000000000000000")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
			},
		},
		{
			name: "keep_null false and NULL seq",
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"min":       models.ParamsValue("10000000000000000000"),
				"max":       models.ParamsValue("100000000000000000000"),
				"keep_null": models.ParamsValue("true"),
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue(nil, true)},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "Implicitly set threshold",
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"min":       models.ParamsValue("0.0"),
				"max":       models.ParamsValue("10.0"),
				"keep_null": models.ParamsValue("false"),
				"decimal":   models.ParamsValue("2"),
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue(nil, true)},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("0.0")
				expectedMax := decimal.RequireFromString("10.0")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
				valStr := val.String()
				matched, err := regexp.MatchString(`^-*\d+[.]*\d{0,2}$`, valStr)
				require.NoError(t, err)
				require.True(t, matched, "value %s does not match regexp for max 2 decimal places", valStr)
			},
		},
		{
			name: "Dynamic mode",
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"keep_null": models.ParamsValue("false"),
				"decimal":   models.ParamsValue("2"),
			},
			dynamicParameter: map[string]models.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue([]byte("1234"), false),
				models.NewColumnRawValue([]byte("-1000020102102"), false),
				models.NewColumnRawValue([]byte("10"), false),
			},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  mysqldbmsdriver.TypeNumeric,
					TypeOID:   mysqldbmsdriver.VirtualOidNumeric,
					TypeClass: models.TypeClassNumeric,
					Length:    0,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				expectedMin := decimal.RequireFromString("-1000020102102")
				expectedMax := decimal.RequireFromString("10")
				var val decimal.Decimal
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val.GreaterThanOrEqual(expectedMin) && val.LessThanOrEqual(expectedMax))
				valStr := val.String()
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
				RandomNumericTransformerDefinition,
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
