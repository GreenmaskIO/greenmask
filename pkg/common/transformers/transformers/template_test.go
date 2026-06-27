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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	coretest "github.com/greenmaskio/greenmask/pkg/common/coretest"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/stretchr/testify/require"
)

func TestTemplateTransformer_Transform(t *testing.T) {
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
			name: "success",
			columns: []core.Column{
				{
					Idx:  0,
					Name: "data",
					Type: core.Type{
						Name:   coretest.TypeInt4,
						ID:     coretest.TypeIDInt4,
						Class:  core.TypeClassInt,
						Length: 0,
						Size:   2,
					},
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("3"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"template": core.ParamsValue(`
					{{- $val := .GetValue -}}
					{{- if isNull $val -}}
					{{- null -}}
					{{- else if eq $val 1 }}
					{{- 123 -}}
					{{- else -}}
					{{- add $val 10 | .EncodeValue -}}
					{{- end -}}
				`),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.Equal(t, int64(13), val)
			},
		},
		{
			name: "validation error",
			columns: []core.Column{
				{
					Idx:  0,
					Name: "data",
					Type: core.Type{
						Name:   coretest.TypeInt4,
						ID:     coretest.TypeIDInt4,
						Class:  core.TypeClassInt,
						Length: 0,
						Size:   2,
					},
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("3"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"template": core.ParamsValue(`
					{{ "asadasd" -}}
				`),
				"validate": core.ParamsValue("true"),
			},
			expectedErr: "validate template output via driver: strconv.ParseInt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				TemplateTransformerDefinition,
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
