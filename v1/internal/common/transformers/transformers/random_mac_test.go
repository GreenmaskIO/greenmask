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
	"net"
	"testing"

	"github.com/greenmaskio/greenmask/internal/generators/transformers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestRandomMacTransformer_Transform(t *testing.T) {
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
			name: "Random mac addr with keepOriginalVendor with Universal and Individual",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"engine":               commonmodels.ParamsValue("deterministic"),
				"keep_original_vendor": commonmodels.ParamsValue("true"),
				"cast_type":            commonmodels.ParamsValue(castTypeNameIndividual),
				"management_type":      commonmodels.ParamsValue(managementTypeNameUniversal),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("00:1a:2b:3c:4d:5e"), false)},
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
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res := &net.HardwareAddr{}
				err = scanMacAddr(val.Data, res)
				require.NoError(t, err)

				newMacAddrInfo, err := transformers.ExploreMacAddress(*res)
				require.NoError(t, err)

				// Test keep original vendor is working
				require.Equal(t, "00:1a:2b:3c:4d:5e"[:8], res.String()[:8])
				assert.Equal(t, newMacAddrInfo.CastType, transformers.CastTypeIndividual)
				assert.Equal(t, newMacAddrInfo.ManagementType, transformers.ManagementTypeUniversal)
			},
		},
		{
			name: "Random mac addr with keepOriginalVendor with Universal and Group",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"engine":               commonmodels.ParamsValue("deterministic"),
				"keep_original_vendor": commonmodels.ParamsValue("true"),
				"cast_type":            commonmodels.ParamsValue(castTypeNameIndividual),
				"management_type":      commonmodels.ParamsValue(managementTypeNameUniversal),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("01:1a:2b:3c:4d:5e"), false)},
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
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res := &net.HardwareAddr{}
				err = scanMacAddr(val.Data, res)
				require.NoError(t, err)

				newMacAddrInfo, err := transformers.ExploreMacAddress(*res)
				require.NoError(t, err)

				// Test keep original vendor is working
				require.Equal(t, "01:1a:2b:3c:4d:5e"[:8], res.String()[:8])

				assert.Equal(t, newMacAddrInfo.CastType, transformers.CastTypeGroup)
				assert.Equal(t, newMacAddrInfo.ManagementType, transformers.ManagementTypeUniversal)
			},
		},
		{
			name: "Random mac addr with keepOriginalVendor with Any and Any",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"engine":               commonmodels.ParamsValue("deterministic"),
				"keep_original_vendor": commonmodels.ParamsValue("true"),
				"cast_type":            commonmodels.ParamsValue(castTypeNameAny),
				"management_type":      commonmodels.ParamsValue(managementTypeNameAny),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("03:1a:2b:3c:4d:5e"), false)},
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
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res := &net.HardwareAddr{}
				err = scanMacAddr(val.Data, res)
				require.NoError(t, err)

				newMacAddrInfo, err := transformers.ExploreMacAddress(*res)
				require.NoError(t, err)

				// Test keep original vendor is working
				require.Equal(t, "03:1a:2b:3c:4d:5e"[:8], res.String()[:8])

				assert.Equal(t, newMacAddrInfo.CastType, transformers.CastTypeGroup)
				assert.Equal(t, newMacAddrInfo.ManagementType, transformers.ManagementTypeLocal)
			},
		},
		{
			name: "Random mac addr without keepOriginalVendor with Universal and Group",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"engine":               commonmodels.ParamsValue("deterministic"),
				"keep_original_vendor": commonmodels.ParamsValue("false"),
				"cast_type":            commonmodels.ParamsValue(castTypeNameGroup),
				"management_type":      commonmodels.ParamsValue(managementTypeNameUniversal),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("03:1a:2b:3c:4d:5e"), false)},
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
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res := &net.HardwareAddr{}
				err = scanMacAddr(val.Data, res)
				require.NoError(t, err)

				newMacAddrInfo, err := transformers.ExploreMacAddress(*res)
				require.NoError(t, err)

				// Test keep original vendor is working
				require.NotEqual(t, "03:1a:2b:3c:4d:5e"[:8], res.String()[:8])

				assert.Equal(t, newMacAddrInfo.CastType, transformers.CastTypeGroup)
				assert.Equal(t, newMacAddrInfo.ManagementType, transformers.ManagementTypeUniversal)
			},
		},
		{
			name: "Random mac addr without keepOriginalVendor with Universal and Individual",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":               commonmodels.ParamsValue("data"),
				"engine":               commonmodels.ParamsValue("deterministic"),
				"keep_original_vendor": commonmodels.ParamsValue("false"),
				"cast_type":            commonmodels.ParamsValue(castTypeNameGroup),
				"management_type":      commonmodels.ParamsValue(managementTypeNameLocal),
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("03:1a:2b:3c:4d:5e"), false)},
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
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res := &net.HardwareAddr{}
				err = scanMacAddr(val.Data, res)
				require.NoError(t, err)

				newMacAddrInfo, err := transformers.ExploreMacAddress(*res)
				require.NoError(t, err)

				// Test keep original vendor is working
				require.NotEqual(t, "03:1a:2b:3c:4d:5e"[:8], res.String()[:8])

				assert.Equal(t, newMacAddrInfo.CastType, transformers.CastTypeGroup)
				assert.Equal(t, newMacAddrInfo.ManagementType, transformers.ManagementTypeLocal)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomMacAddressDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, utils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, utils.PrintValidationWarnings(ctx, vc, nil, true))
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			require.NoError(t, utils.PrintValidationWarnings(ctx, vc, nil, true))
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
