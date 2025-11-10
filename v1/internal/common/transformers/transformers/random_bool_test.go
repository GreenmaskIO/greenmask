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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestRandomBoolTransformer_Transform(t *testing.T) {

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
			name: "common",
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBoolean,
					TypeOID:   mysqldbmsdriver.VirtualOidBoolean,
					TypeClass: commonmodels.TypeClassBoolean,
					Length:    0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("t"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column": commonmodels.ParamsValue("data"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var value bool
				isNull, err := recorder.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				assert.False(t, isNull)
			},
		},
		{
			name: "keep_null false and NULL seq",
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBoolean,
					TypeOID:   mysqldbmsdriver.VirtualOidBoolean,
					TypeClass: commonmodels.TypeClassBoolean,
					Length:    0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"keep_null": commonmodels.ParamsValue("false"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var value bool
				isNull, err := recorder.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				assert.False(t, isNull)
			},
		},
		{
			name: "keep_null true and NULL seq",
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBoolean,
					TypeOID:   mysqldbmsdriver.VirtualOidBoolean,
					TypeClass: commonmodels.TypeClassBoolean,
					Length:    0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var value bool
				isNull, err := recorder.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				assert.True(t, isNull)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				BoolTransformerDefinition,
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
