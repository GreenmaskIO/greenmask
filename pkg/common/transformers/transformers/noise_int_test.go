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
	"math"
	"testing"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	coretest "github.com/greenmaskio/greenmask/pkg/common/coretest"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: Test the max/min value exceeded
func TestNoiseIntTransformer_Transform(t *testing.T) {
	// Positive cases
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
			name: "int8",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(12))
				assert.LessOrEqual(t, val, int64(234))
				log.Info().Int64("value", val).Msg("Transformed value")
			},
			original: []*core.ColumnRawValue{core.NewColumnRawValue([]byte("123"), false)},
		},
		{
			name: "int8",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
				"min":       core.ParamsValue("0"),
				"max":       core.ParamsValue("110"),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(0))
				assert.LessOrEqual(t, val, int64(110))
				log.Info().Int64("value", val).Msg("Transformed value")
			},
			original: []*core.ColumnRawValue{core.NewColumnRawValue([]byte("123"), false)},
		},
		{
			name: "dynamic mode",
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]core.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(0))
				assert.LessOrEqual(t, val, int64(110))
				log.Info().Int64("value", val).Msg("Transformed value")
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false),
				core.NewColumnRawValue([]byte("0"), false),
				core.NewColumnRawValue([]byte("110"), false),
			},
		},
		{
			name: "dynamic mode with min and empty max",
			// This should use the default max value for float8.
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]core.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false),
				core.NewColumnRawValue([]byte("0"), false),
				core.NewColumnRawValue([]byte("110"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record core.Recorder) {
				var value int64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between 0 and 110 and in the min and max ratio range
				assert.GreaterOrEqual(t, int(value), 0)
				assert.LessOrEqual(t, int(value), math.MaxInt64)
			},
		},
		{
			name: "dynamic mode with max and empty min",
			// This should use the default max value for float8.
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min_ratio": core.ParamsValue("0.2"),
				"max_ratio": core.ParamsValue("0.9"),
			},
			dynamicParameter: map[string]core.DynamicParamValue{
				"max": {
					Column: "max_val",
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false),
				core.NewColumnRawValue([]byte("0"), false),
				core.NewColumnRawValue([]byte("110"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  coretest.TypeInt4,
					TypeID:    coretest.TypeIDInt4,
					TypeClass: core.TypeClassInt,
					Size:      8,
				},
			},
			validateFn: func(t *testing.T, record core.Recorder) {
				var value int64
				isNull, err := record.ScanColumnValueByName("data", &value)
				require.NoError(t, err)
				require.False(t, isNull)
				// The value should be between 0 and 110 and in the min and max ratio range
				assert.GreaterOrEqual(t, int(value), math.MinInt64)
				assert.LessOrEqual(t, int(value), 110)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				NoiseIntTransformerDefinition,
				tt.columns,
				tt.staticParameters,
				tt.dynamicParameter,
			)
			err := env.InitParameters(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			err = env.InitTransformer(t, ctx)
			require.NoError(t, err)
			require.False(t, vc.HasWarnings())

			env.SetRecord(t, tt.original...)

			err = env.Transform(t, ctx)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			tt.validateFn(t, env.GetRecord())
		})
	}
}
