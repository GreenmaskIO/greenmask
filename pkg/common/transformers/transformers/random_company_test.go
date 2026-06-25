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
	"github.com/greenmaskio/greenmask/pkg/common/transformers/generators/transformers"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestRandomCompanyTransformer_Transform(t *testing.T) {
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
					TypeName:  coretest.TypeText,
					TypeClass: core.TypeClassText,
					TypeID:    coretest.TypeIDText,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("Some Inc."), false)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					randomCompanyNameColumn{
						Name:     "data",
						Template: "{{ .CompanyName }} {{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
					},
				),
				"engine": core.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanyNames)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanySuffixes)
			},
		},
		{
			name: "keep_null and original is not null",
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
				core.NewColumnRawValue([]byte("Some Inc."), false)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					randomCompanyNameColumn{
						Name:     "data",
						Template: "{{ .CompanyName }} {{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
						KeepNull: utils.New(true),
					},
				),
				"engine": core.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanyNames)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanySuffixes)
			},
		},
		{
			name: "keep_null and original is not null",
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
				core.NewColumnRawValue(nil, true)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					randomCompanyNameColumn{
						Name:     "data",
						Template: "{{ .CompanyName }} {{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
						KeepNull: utils.New(false),
					},
				),
				"engine": core.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanyNames)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanySuffixes)
			},
		},
		{
			name: "keep_null and original is null multiple columns",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "name",
					TypeName:  coretest.TypeText,
					TypeClass: core.TypeClassText,
					TypeID:    coretest.TypeIDText,
					Length:    0,
				},
				{
					Idx:       1,
					Name:      "suffix",
					TypeName:  coretest.TypeText,
					TypeClass: core.TypeClassText,
					TypeID:    coretest.TypeIDText,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("some"), false),
				core.NewColumnRawValue(nil, true),
			},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					randomCompanyNameColumn{
						Name:     "name",
						Template: "{{ .CompanyName }}",
						Hashing:  true,
						HashOnly: false,
						KeepNull: utils.New(true),
					},
					randomCompanyNameColumn{
						Name:     "suffix",
						Template: "{{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
						KeepNull: utils.New(true),
					},
				),
				"engine": core.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("name")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanyNames)

				rawVal, err = recorder.GetRawColumnValueByName("suffix")
				require.NoError(t, err)
				require.True(t, rawVal.IsNull)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomCompanyTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, utils.PrintValidationWarnings(ctx, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, utils.PrintValidationWarnings(ctx, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			require.NoError(t, utils.PrintValidationWarnings(ctx, nil, true))
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
