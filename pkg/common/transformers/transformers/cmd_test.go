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
	"encoding/json"
	"testing"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/transformers/cmd"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCMD_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]core.ParamsValue
		dynamicParameter map[string]core.DynamicParamValue
		original         []*core.ColumnRawValue
		validateFn       func(t *testing.T, recorder core.Recorder)
		expectedErr      string
		columns          []core.Column
		expectedDoneErr  string
	}{
		{
			name: "success",
			columns: []core.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeID:   mysqldbmsdriver.TypeIDText,
					Length:   0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("Some Inc."), false)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": core.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": utils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           core.ParamsValue("true"),
				"timeout":            core.ParamsValue("5s"),
				"expected_exit_code": core.ParamsValue("-1"),
				"skip_on_behaviour":  core.ParamsValue("all"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				assert.Equal(t, "8da047571f03062fc0abf747d86b3fce", string(rawVal.Data))
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
			},
		},
		{
			name: "timeout err",
			columns: []core.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeID:   mysqldbmsdriver.TypeIDText,
					Length:   0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("Some Inc."), false)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": core.ParamsValue("cmd/test/transformer-timeout.sh"),
				"driver": utils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           core.ParamsValue("true"),
				"timeout":            core.ParamsValue("500ms"),
				"expected_exit_code": core.ParamsValue("-1"),
				"skip_on_behaviour":  core.ParamsValue("all"),
			},
			expectedErr: context.DeadlineExceeded.Error(),
		},
		{
			name: "validate err",
			columns: []core.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeInt,
					TypeID:   mysqldbmsdriver.TypeIDInt,
					Length:   0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": core.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": utils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           core.ParamsValue("true"),
				"timeout":            core.ParamsValue("5s"),
				"expected_exit_code": core.ParamsValue("-1"),
				"skip_on_behaviour":  core.ParamsValue("all"),
			},
			expectedErr: core.ErrValueValidationFailed.Error(),
		},
		{
			name: "unexpected exit code",
			columns: []core.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeID:   mysqldbmsdriver.TypeIDText,
					Length:   0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": core.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": utils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           core.ParamsValue("true"),
				"timeout":            core.ParamsValue("5s"),
				"expected_exit_code": core.ParamsValue("0"),
				"skip_on_behaviour":  core.ParamsValue("all"),
			},
			expectedDoneErr: ErrUnexpectedCmdExitCode.Error(),
			validateFn:      func(t *testing.T, recorder core.Recorder) {},
		},
		{
			name: "skip",
			columns: []core.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeID:   mysqldbmsdriver.TypeIDText,
					Length:   0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			staticParameters: map[string]core.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  true,
						SkipOriginalData: false,
					},
				),
				"executable": core.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": utils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           core.ParamsValue("true"),
				"timeout":            core.ParamsValue("5s"),
				"expected_exit_code": core.ParamsValue("-1"),
				"skip_on_behaviour":  core.ParamsValue("all"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
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
				CMDTransformerDefinition,
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

			defer func() {
				err := env.transformer.Done(ctx)
				if tt.expectedDoneErr != "" {
					require.ErrorContains(t, err, tt.expectedErr)
					return
				} else {
					require.NoError(t, err)
				}
			}()

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
