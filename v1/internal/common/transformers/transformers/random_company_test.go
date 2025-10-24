package transformers

import (
	"context"
	"testing"

	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestRandomCompanyTransformer_Transform(t *testing.T) {
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
			name: "numeric",
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
					randomCompanyNameColumn{
						Name:     "data",
						Template: "{{ .CompanyName }} {{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
					},
				),
				"engine": commonmodels.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
					randomCompanyNameColumn{
						Name:     "data",
						Template: "{{ .CompanyName }} {{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
						KeepNull: commonutils.New(true),
					},
				),
				"engine": commonmodels.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
					randomCompanyNameColumn{
						Name:     "data",
						Template: "{{ .CompanyName }} {{ .CompanySuffix }}",
						Hashing:  true,
						HashOnly: false,
						KeepNull: commonutils.New(false),
					},
				),
				"engine": commonmodels.ParamsValue("deterministic"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
				log.Debug().Str("Result", string(rawVal.Data)).Msg("Generated data")
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanyNames)
				assertStringContainsOneOfItemFromList(t, string(rawVal.Data), transformers.DefaultCompanySuffixes)
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
