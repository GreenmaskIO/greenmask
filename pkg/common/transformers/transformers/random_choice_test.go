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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	coretest "github.com/greenmaskio/greenmask/pkg/common/coretest"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomChoiceTransformer_Transform(t *testing.T) {
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
			name: "success date type",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeDate,
					TypeID:    coretest.TypeIDDate,
					TypeClass: core.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("2023-11-10"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"engine":   core.ParamsValue("random"),
				"values":   core.ParamsValue(`["2023-11-10", "2023-01-01", "2023-01-02"]`),
				"validate": core.ParamsValue(`true`),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val string
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val == "2023-11-10" || val == "2023-01-01" || val == "2023-01-02")
			},
		},
		{
			name: "success json type",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeText,
					TypeClass: core.TypeClassText,
					TypeID:    coretest.TypeIDText,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("2023-11-10"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"engine":   core.ParamsValue("random"),
				"values":   core.ParamsValue(`[{"a": 1}, {"b": 2}, {"c": 3}]`),
				"validate": core.ParamsValue(`true`),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
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
		columns := []core.Column{
			{
				Idx:      0,
				Name:     "data",
				TypeName: coretest.TypeDate,
				TypeID:   coretest.TypeIDDate,
				Length:   0,
			},
		}
		staticParameters := map[string]core.ParamsValue{
			"column":   core.ParamsValue("data"),
			"engine":   core.ParamsValue("random"),
			"values":   core.ParamsValue(`["INVALID_DATE"]`),
			"validate": core.ParamsValue(`true`),
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
		assert.ErrorIs(t, err, core.ErrFatalValidationError)
		require.True(t, vc.HasWarnings())
	})
}
