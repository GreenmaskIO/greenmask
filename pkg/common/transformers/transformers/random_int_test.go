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

func TestRandomIntTransformer_Transform(t *testing.T) {
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
			name: "int2",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt2,
					TypeID:    coretest.TypeIDInt2,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      2,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"min":    core.ParamsValue("1"),
				"max":    core.ParamsValue("100"),
				"engine": core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1))
				assert.LessOrEqual(t, val, int64(100))
			},
		},
		{
			name: "int4",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      4,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"min":    core.ParamsValue("1"),
				"max":    core.ParamsValue("100"),
				"engine": core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1))
				assert.LessOrEqual(t, val, int64(100))

			},
		},
		{
			name: "int8",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt8,
					TypeID:    coretest.TypeIDInt8,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("12345"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"min":    core.ParamsValue("1"),
				"max":    core.ParamsValue("100"),
				"engine": core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1))
				assert.LessOrEqual(t, val, int64(100))

			},
		},
		{
			name: "keep_null true and NULL seq",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"min":       core.ParamsValue("1"),
				"max":       core.ParamsValue("100"),
				"keep_null": core.ParamsValue("true"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt8,
					TypeID:    coretest.TypeIDInt8,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val float64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "keep_null true and not NULL seq",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"min":       core.ParamsValue("1"),
				"max":       core.ParamsValue("100"),
				"keep_null": core.ParamsValue("true"),
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("12345"), false)},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt8,
					TypeID:    coretest.TypeIDInt8,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
			},
		},
		{
			name: "dynamic mode",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"engine":    core.ParamsValue("deterministic"),
				"keep_null": core.ParamsValue("false"),
			},
			dynamicParameter: map[string]core.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("1234"), false),
				core.NewColumnRawValue([]byte("1"), false),
				core.NewColumnRawValue([]byte("100"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt8,
					TypeID:    coretest.TypeIDInt8,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  coretest.TypeInt8,
					TypeID:    coretest.TypeIDInt8,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  coretest.TypeInt8,
					TypeID:    coretest.TypeIDInt8,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var expectedMin int64 = 1
				var expectedMax int64 = 10
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				require.True(t, val >= expectedMin && val <= expectedMax)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomIntegerTransformerDefinition,
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
