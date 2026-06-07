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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnixTimestampTransformer_Transform(t *testing.T) {
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
			name: "seconds",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"min":      core.ParamsValue("1616842649"),
				"max":      core.ParamsValue("1711537049"),
				"unit":     core.ParamsValue(secondsUnit),
				"min_unit": core.ParamsValue(secondsUnit),
				"max_unit": core.ParamsValue(secondsUnit),
				"engine":   core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1616842649))
				assert.LessOrEqual(t, val, int64(1711537049))
			},
		},
		{
			name: "milliseconds",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"min":      core.ParamsValue("1611546399134"),
				"max":      core.ParamsValue("1711546399134"),
				"unit":     core.ParamsValue(milliUnit),
				"min_unit": core.ParamsValue(milliUnit),
				"max_unit": core.ParamsValue(milliUnit),
				"engine":   core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1611546399134))
				assert.LessOrEqual(t, val, int64(1711546399134))
			},
		},
		{
			name: "microseconds",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"min":      core.ParamsValue("1611546399134"),
				"max":      core.ParamsValue("1711546399134"),
				"unit":     core.ParamsValue(microUnit),
				"min_unit": core.ParamsValue(microUnit),
				"max_unit": core.ParamsValue(microUnit),
				"engine":   core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1611546399134123))
				assert.LessOrEqual(t, val, int64(1711546399134123))
			},
		},
		{
			name: "nanoseconds",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"min":      core.ParamsValue("1616842649000000000"),
				"max":      core.ParamsValue("1716842649000000000"),
				"unit":     core.ParamsValue(nanoUnit),
				"min_unit": core.ParamsValue(nanoUnit),
				"max_unit": core.ParamsValue(nanoUnit),
				"engine":   core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1616842649000000000))
				assert.LessOrEqual(t, val, int64(1716842649000000000))
			},
		},
		{
			name: "nanoseconds_truncate",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min":       core.ParamsValue("1616842649000000000"),
				"max":       core.ParamsValue("1716842649000000000"),
				"unit":      core.ParamsValue(nanoUnit),
				"min_unit":  core.ParamsValue(nanoUnit),
				"max_unit":  core.ParamsValue(nanoUnit),
				"engine":    core.ParamsValue("random"),
				"keep_null": core.ParamsValue("true"),
				"truncate":  core.ParamsValue("day"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				assert.GreaterOrEqual(t, val, int64(1639692000000000000))
				assert.LessOrEqual(t, val, int64(1739692000000000000))
				// Cast int64 nanoseconds to time.Time and check that hour, min, sec, nsec are zeroed
				tm := getTimeByUnit(val, nanoUnit)
				require.Equal(t, 0, tm.Hour())
				require.Equal(t, 0, tm.Minute())
				require.Equal(t, 0, tm.Second())
				require.Equal(t, 0, tm.Nanosecond())
			},
		},
		{
			name: "keep_null true and original is null",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min":       core.ParamsValue("1616842649000000000"),
				"max":       core.ParamsValue("1716842649000000000"),
				"unit":      core.ParamsValue(nanoUnit),
				"min_unit":  core.ParamsValue(nanoUnit),
				"max_unit":  core.ParamsValue(nanoUnit),
				"engine":    core.ParamsValue("random"),
				"keep_null": core.ParamsValue("true"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "keep_null true and original is not null",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min":       core.ParamsValue("1616842649000000000"),
				"max":       core.ParamsValue("1716842649000000000"),
				"unit":      core.ParamsValue(nanoUnit),
				"min_unit":  core.ParamsValue(nanoUnit),
				"max_unit":  core.ParamsValue(nanoUnit),
				"engine":    core.ParamsValue("random"),
				"keep_null": core.ParamsValue("true"),
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
				"column":   core.ParamsValue("data"),
				"unit":     core.ParamsValue(nanoUnit),
				"min_unit": core.ParamsValue(nanoUnit),
				"max_unit": core.ParamsValue(nanoUnit),
				"engine":   core.ParamsValue("random"),
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
				core.NewColumnRawValue([]byte("1616842649000000000"), false),
				core.NewColumnRawValue([]byte("1716842649000000000"), false),
			},
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
				{
					Idx:       1,
					Name:      "min_val",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
					TypeClass: core.TypeClassInt,
					Length:    0,
					Size:      8,
				},
				{
					Idx:       2,
					Name:      "max_val",
					TypeName:  mysqldbmsdriver.TypeBigInt,
					TypeOID:   mysqldbmsdriver.VirtualOidBigInt,
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
				assert.GreaterOrEqual(t, val, int64(1616842649000000000))
				assert.LessOrEqual(t, val, int64(1716842649000000000))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				UnixTimestampTransformerDefinition,
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
