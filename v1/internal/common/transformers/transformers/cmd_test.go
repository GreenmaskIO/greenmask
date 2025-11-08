package transformers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	cmd "github.com/greenmaskio/greenmask/v1/internal/common/transformers/transformers/cmd"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestCMD_Transform(t *testing.T) {
	tests := []struct {
		name             string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		original         []*commonmodels.ColumnRawValue
		validateFn       func(t *testing.T, recorder commonininterfaces.Recorder)
		expectedErr      string
		columns          []commonmodels.Column
		expectedDoneErr  string
	}{
		{
			name: "success",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeOID:  mysqldbmsdriver.VirtualOidText,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("Some Inc."), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": commonmodels.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": commonutils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           commonmodels.ParamsValue("true"),
				"timeout":            commonmodels.ParamsValue("2s"),
				"expected_exit_code": commonmodels.ParamsValue("-1"),
				"skip_on_behaviour":  commonmodels.ParamsValue("all"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				assert.Equal(t, "8da047571f03062fc0abf747d86b3fce", string(rawVal.Data))
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
			},
		},
		{
			name: "timeout err",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeOID:  mysqldbmsdriver.VirtualOidText,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("Some Inc."), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": commonmodels.ParamsValue("cmd/test/transformer-timeout.sh"),
				"driver": commonutils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           commonmodels.ParamsValue("true"),
				"timeout":            commonmodels.ParamsValue("500ms"),
				"expected_exit_code": commonmodels.ParamsValue("-1"),
				"skip_on_behaviour":  commonmodels.ParamsValue("all"),
			},
			expectedErr: context.DeadlineExceeded.Error(),
		},
		{
			name: "validate err",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeInt,
					TypeOID:  mysqldbmsdriver.VirtualOidInt,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": commonmodels.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": commonutils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           commonmodels.ParamsValue("true"),
				"timeout":            commonmodels.ParamsValue("2s"),
				"expected_exit_code": commonmodels.ParamsValue("-1"),
				"skip_on_behaviour":  commonmodels.ParamsValue("all"),
			},
			expectedErr: commonmodels.ErrValueValidationFailed.Error(),
		},
		{
			name: "unexpected exit code",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeOID:  mysqldbmsdriver.VirtualOidText,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  false,
						SkipOriginalData: false,
					},
				),
				"executable": commonmodels.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": commonutils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           commonmodels.ParamsValue("true"),
				"timeout":            commonmodels.ParamsValue("2s"),
				"expected_exit_code": commonmodels.ParamsValue("0"),
				"skip_on_behaviour":  commonmodels.ParamsValue("all"),
			},
			expectedDoneErr: ErrUnexpectedCmdExitCode.Error(),
			validateFn:      func(t *testing.T, recorder commonininterfaces.Recorder) {},
		},
		{
			name: "skip",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeOID:  mysqldbmsdriver.VirtualOidText,
					Length:   0,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					cmdColumnContainer{
						Name:             "data",
						NotAffected:      false,
						SkipOnNullInput:  true,
						SkipOriginalData: false,
					},
				),
				"executable": commonmodels.ParamsValue("cmd/test/transformer-success.sh"),
				"driver": commonutils.Must(json.Marshal(&cmd.RowDriverSetting{
					Name: cmd.RowDriverNameText,
				})),
				"validate":           commonmodels.ParamsValue("true"),
				"timeout":            commonmodels.ParamsValue("2s"),
				"expected_exit_code": commonmodels.ParamsValue("-1"),
				"skip_on_behaviour":  commonmodels.ParamsValue("all"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, commonutils.PrintValidationWarnings(ctx, vc, nil, true))
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
