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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	coretest "github.com/greenmaskio/greenmask/pkg/common/coretest"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/stretchr/testify/require"
)

func TestRandomIP_Transform(t *testing.T) {
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
			name: "dynamic",
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"engine": core.ParamsValue("random"),
			},
			dynamicParameter: map[string]core.DynamicParamValue{
				"subnet": {
					Column: "subnet",
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("192.168.1.1"), false),
				core.NewColumnRawValue([]byte("192.168.1.1/24"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeText,
					TypeID:    coretest.TypeIDText,
					TypeClass: core.TypeClassText,
					Length:    -1,
				},
				{
					Idx:      1,
					Name:     "subnet",
					TypeName: coretest.TypeText,
					TypeID:   coretest.TypeIDText,
					Length:   -1,
				},
			},
			validateFn: func(t *testing.T, record core.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				_, subnet, err := net.ParseCIDR("192.168.1.1/24")
				require.NoError(t, err)
				ip := net.ParseIP(string(val.Data))
				require.NotNil(t, ip)
				require.True(t, subnet.Contains(ip))
			},
		},
		{
			name: "static",
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"subnet": core.ParamsValue("192.168.1.1/24"),
				"engine": core.ParamsValue("random"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("192.168.1.1"), false)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeText,
					TypeID:    coretest.TypeIDText,
					TypeClass: core.TypeClassText,
					Length:    4,
				},
			},
			validateFn: func(t *testing.T, record core.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				_, subnet, err := net.ParseCIDR("192.168.1.1/24")
				require.NoError(t, err)
				ip := net.ParseIP(string(val.Data))
				require.NotNil(t, ip)
				require.True(t, subnet.Contains(ip))
			},
		},
		{
			name: "static deterministic",
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"subnet": core.ParamsValue("192.168.1.1/24"),
				"engine": core.ParamsValue("deterministic"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("192.168.1.1"), false)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeText,
					TypeID:    coretest.TypeIDText,
					TypeClass: core.TypeClassText,
					Length:    4,
				},
			},
			validateFn: func(t *testing.T, record core.Recorder) {
				val, err := record.GetRawColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				_, subnet, err := net.ParseCIDR("192.168.1.1/24")
				require.NoError(t, err)
				ip := net.ParseIP(string(val.Data))
				require.NotNil(t, ip)
				require.True(t, subnet.Contains(ip))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomIPDefinition,
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
