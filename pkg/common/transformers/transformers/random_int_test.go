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

	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomIntTransformer_Transform(t *testing.T) {
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
			name: "int2",
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeSmallInt,
					TypeOID:   mysqldbmsdriver.VirtualOidSmallInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      2,
				},
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"min":    models.ParamsValue("1"),
				"max":    models.ParamsValue("100"),
				"engine": models.ParamsValue("random"),
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
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeMediumInt,
					TypeOID:   mysqldbmsdriver.VirtualOidMediumInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      4,
				},
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"min":    models.ParamsValue("1"),
				"max":    models.ParamsValue("100"),
				"engine": models.ParamsValue("random"),
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
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]models.ParamsValue{
				"column": models.ParamsValue("data"),
				"min":    models.ParamsValue("1"),
				"max":    models.ParamsValue("100"),
				"engine": models.ParamsValue("random"),
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
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"min":       models.ParamsValue("1"),
				"max":       models.ParamsValue("100"),
				"keep_null": models.ParamsValue("true"),
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue(nil, true)},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      8,
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
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"min":       models.ParamsValue("1"),
				"max":       models.ParamsValue("100"),
				"keep_null": models.ParamsValue("true"),
			},
			original: []*models.ColumnRawValue{
				models.NewColumnRawValue([]byte("12345"), false)},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      8,
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
			staticParameters: map[string]models.ParamsValue{
				"column":    models.ParamsValue("data"),
				"engine":    models.ParamsValue("deterministic"),
				"keep_null": models.ParamsValue("false"),
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
				models.NewColumnRawValue([]byte("1"), false),
				models.NewColumnRawValue([]byte("100"), false),
			},
			columns: []models.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: models.TypeClassInt,
					Length:    0,
					Size:      8,
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
