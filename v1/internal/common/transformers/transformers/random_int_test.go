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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestRandomIntTransformer_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		original         []*commonmodels.ColumnRawValue
		validateFn       func(t *testing.T, recorder commonininterfaces.Recorder)
		expectedErr      string
		columns          []commonmodels.Column
	}{
		{
			name: "int2",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeSmallInt,
					TypeOID:  mysqldbmsdriver.VirtualOidSmallInt,
					Length:   0,
					Size:     2,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
				"min":    commonmodels.ParamsValue("1"),
				"max":    commonmodels.ParamsValue("100"),
				"engine": commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1))
				assert.LessOrEqual(t, val, int64(100))
			},
		},
		{
			name: "int4",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeMediumInt,
					TypeOID:  mysqldbmsdriver.VirtualOidMediumInt,
					Length:   0,
					Size:     4,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
				"min":    commonmodels.ParamsValue("1"),
				"max":    commonmodels.ParamsValue("100"),
				"engine": commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1))
				assert.LessOrEqual(t, val, int64(100))

			},
		},
		{
			name: "int8",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
				"min":    commonmodels.ParamsValue("1"),
				"max":    commonmodels.ParamsValue("100"),
				"engine": commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1))
				assert.LessOrEqual(t, val, int64(100))

			},
		},
		{
			name: "keep_null true and NULL seq",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"min":       commonmodels.ParamsValue("1"),
				"max":       commonmodels.ParamsValue("100"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "keep_null true and not NULL seq",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"min":       commonmodels.ParamsValue("1"),
				"max":       commonmodels.ParamsValue("100"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("12345"), false)},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
			},
		},
		{
			name: "dynamic mode",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"engine":    commonmodels.ParamsValue("deterministic"),
				"keep_null": commonmodels.ParamsValue("false"),
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
				commonmodels.NewColumnRawValue([]byte("1234"), false),
				commonmodels.NewColumnRawValue([]byte("1"), false),
				commonmodels.NewColumnRawValue([]byte("100"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
				{
					Idx:      1,
					Name:     "min_val",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
				{
					Idx:      2,
					Name:     "max_val",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var expectedMin int64 = 1
				var expectedMax int64 = 10
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val >= expectedMin && val <= expectedMax)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomIntegerTransformerDefinition,
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
