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

func TestTemplateRecordTransformer_Transform(t *testing.T) {
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
			name: "success with date",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeDateTime,
					TypeOID:  mysqldbmsdriver.VirtualOidDateTime,
					Length:   0,
					Size:     2,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte(""), true)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"template": commonmodels.ParamsValue(`
					 {{ $val := .GetColumnValue "data" }}
					  {{ if isNull $val }}
						{{ "2023-11-20 01:00:00" | .DecodeValueByColumn "data" | dateModify "24h" | .SetColumnValue "data" }}
					  {{ else }}
						 {{ "2023-11-20 01:00:00" | .DecodeValueByColumn "data" | dateModify "48h" | .SetColumnValue "data" }}
					  {{ end }}
				`),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				data, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.Equal(t, "2023-11-21 01:00:00", string(data.Data))
			},
		},
		{
			name: "success json",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeText,
					TypeOID:  mysqldbmsdriver.VirtualOidText,
					Length:   0,
					Size:     2,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("{\"name\": \"test\"}"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"template": commonmodels.ParamsValue(`
					{{ $val := .GetRawColumnValue "data" }}
					{{ jsonSet "name" "hello" $val | jsonValidate | .SetColumnValue "data" }}
				`),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				data, err := recorder.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.Equal(t, "{\"name\": \"hello\"}", string(data.Data))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				TemplateRecordTransformerDefinition,
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
