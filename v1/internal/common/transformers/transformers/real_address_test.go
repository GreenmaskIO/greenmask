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

	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestRealAddressTransformer_Transform(t *testing.T) {
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
			name: "seconds",
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
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"columns": dumpColumnContainers(
					realAddressColumn{
						Name:     "data",
						Template: "{{ .Address }} {{ .City }} {{ .State }} {{ .PostalCode }} {{ .Latitude }} {{ .Longitude }}",
						//Hashing:  true,
						//HashOnly: false,
					},
				),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				rawVal, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, rawVal.IsNull)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RealAddressTransformerDefinition,
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

//func TestRealAddressTransformer_Transform(t *testing.T) {
//	driver, record := getDriverAndRecord("data", "somaval")
//
//	columns := []*realAddressColumn{
//		{
//			Name:     "data",
//			Template: "{{ .Address }} {{ .City }} {{ .State }} {{ .PostalCode }} {{ .Latitude }} {{ .Longitude }}",
//		},
//	}
//
//	rawData, err := json.Marshal(columns)
//	require.NoError(t, err)
//
//	transformer, warnings, err := RealAddressTransformerDefinition.Instance(
//		context.Background(),
//		driver,
//		map[string]toolkit.ParamsValue{
//			"columns": rawData,
//		},
//		nil,
//		"",
//	)
//
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//
//	_, err = transformer.Transformer.Transform(context.Background(), record)
//	require.NoError(t, err)
//	rawValue, err := record.GetRawColumnValueByName("data")
//	require.NoError(t, err)
//	require.False(t, rawValue.IsValueNull)
//	require.Regexp(t, `.* \d+ \-?\d+.\d+ \-?\d+.\d+`, string(rawValue.Data))
//}
//
//func TestMakeNewFakeTransformerFunction_parsing_error(t *testing.T) {
//	driver, _ := getDriverAndRecord("data", "somaval")
//
//	columns := []*realAddressColumn{
//		{
//			Name:     "data",
//			Template: "{{ .Address }",
//		},
//	}
//
//	rawData, err := json.Marshal(columns)
//	require.NoError(t, err)
//
//	_, warnings, err := RealAddressTransformerDefinition.Instance(
//		context.Background(),
//		driver,
//		map[string]toolkit.ParamsValue{
//			"columns": rawData,
//		},
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Len(t, warnings, 1)
//	require.Equal(t, "error parsing template", warnings[0].Msg)
//}
//
//func TestMakeNewFakeTransformerFunction_validation_error(t *testing.T) {
//	driver, _ := getDriverAndRecord("data", "somaval")
//
//	columns := []*realAddressColumn{
//		{
//			Name:     "data",
//			Template: "{{ .Address1 }}",
//		},
//	}
//
//	rawData, err := json.Marshal(columns)
//	require.NoError(t, err)
//
//	_, warnings, err := RealAddressTransformerDefinition.Instance(
//		context.Background(),
//		driver,
//		map[string]toolkit.ParamsValue{
//			"columns": rawData,
//		},
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Len(t, warnings, 1)
//	require.Equal(t, "error validating template", warnings[0].Msg)
//}
