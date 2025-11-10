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

func TestRandomChoiceTransformer_Transform(t *testing.T) {
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
			name: "success date type",
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeDate,
					TypeOID:   mysqldbmsdriver.VirtualOidDate,
					TypeClass: commonmodels.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("2023-11-10"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"engine":   commonmodels.ParamsValue("random"),
				"values":   commonmodels.ParamsValue(`["2023-11-10", "2023-01-01", "2023-01-02"]`),
				"validate": commonmodels.ParamsValue(`true`),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val string
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val == "2023-11-10" || val == "2023-01-01" || val == "2023-01-02")
			},
		},
		{
			name: "success json type",
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeText,
					TypeClass: commonmodels.TypeClassText,
					TypeOID:   mysqldbmsdriver.VirtualOidText,
					Length:    0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("2023-11-10"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"engine":   commonmodels.ParamsValue("random"),
				"values":   commonmodels.ParamsValue(`[{"a": 1}, {"b": 2}, {"c": 3}]`),
				"validate": commonmodels.ParamsValue(`true`),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val string
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val == `{"a": 1}` || val == `{"b": 2}` || val == `{"c": 3}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				ChoiceTransformerDefinition,
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

func TestNewRandomChoiceTransformer(t *testing.T) {
	t.Run("validation failure", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:      0,
				Name:     "data",
				TypeName: mysqldbmsdriver.TypeDate,
				TypeOID:  mysqldbmsdriver.VirtualOidDate,
				Length:   0,
			},
		}
		staticParameters := map[string]commonmodels.ParamsValue{
			"column":   commonmodels.ParamsValue("data"),
			"engine":   commonmodels.ParamsValue("random"),
			"values":   commonmodels.ParamsValue(`["INVALID_DATE"]`),
			"validate": commonmodels.ParamsValue(`true`),
		}
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		env := newTransformerTestEnvReal(t,
			ChoiceTransformerDefinition,
			columns,
			staticParameters,
			nil,
		)
		err := env.InitParameters(t, ctx)
		require.NoError(t, commonutils.PrintValidationWarnings(ctx, nil, true))
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())

		err = env.InitTransformer(t, ctx)
		require.NoError(t, commonutils.PrintValidationWarnings(ctx, nil, true))
		require.Error(t, err)
		assert.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		require.True(t, vc.HasWarnings())
	})
}
