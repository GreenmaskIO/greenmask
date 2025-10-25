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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestUnixTimestampTransformer_Transform(t *testing.T) {
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
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"min":      commonmodels.ParamsValue("1616842649"),
				"max":      commonmodels.ParamsValue("1711537049"),
				"unit":     commonmodels.ParamsValue(secondsUnit),
				"min_unit": commonmodels.ParamsValue(secondsUnit),
				"max_unit": commonmodels.ParamsValue(secondsUnit),
				"engine":   commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"min":      commonmodels.ParamsValue("1611546399134"),
				"max":      commonmodels.ParamsValue("1711546399134"),
				"unit":     commonmodels.ParamsValue(milliUnit),
				"min_unit": commonmodels.ParamsValue(milliUnit),
				"max_unit": commonmodels.ParamsValue(milliUnit),
				"engine":   commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"min":      commonmodels.ParamsValue("1611546399134"),
				"max":      commonmodels.ParamsValue("1711546399134"),
				"unit":     commonmodels.ParamsValue(microUnit),
				"min_unit": commonmodels.ParamsValue(microUnit),
				"max_unit": commonmodels.ParamsValue(microUnit),
				"engine":   commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"min":      commonmodels.ParamsValue("1616842649000000000"),
				"max":      commonmodels.ParamsValue("1716842649000000000"),
				"unit":     commonmodels.ParamsValue(nanoUnit),
				"min_unit": commonmodels.ParamsValue(nanoUnit),
				"max_unit": commonmodels.ParamsValue(nanoUnit),
				"engine":   commonmodels.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min":       commonmodels.ParamsValue("1616842649000000000"),
				"max":       commonmodels.ParamsValue("1716842649000000000"),
				"unit":      commonmodels.ParamsValue(nanoUnit),
				"min_unit":  commonmodels.ParamsValue(nanoUnit),
				"max_unit":  commonmodels.ParamsValue(nanoUnit),
				"engine":    commonmodels.ParamsValue("random"),
				"keep_null": commonmodels.ParamsValue("true"),
				"truncate":  commonmodels.ParamsValue("day"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue(nil, true)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min":       commonmodels.ParamsValue("1616842649000000000"),
				"max":       commonmodels.ParamsValue("1716842649000000000"),
				"unit":      commonmodels.ParamsValue(nanoUnit),
				"min_unit":  commonmodels.ParamsValue(nanoUnit),
				"max_unit":  commonmodels.ParamsValue(nanoUnit),
				"engine":    commonmodels.ParamsValue("random"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
		{
			name: "keep_null true and original is not null",
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("123"), false)},
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":    commonmodels.ParamsValue("data"),
				"min":       commonmodels.ParamsValue("1616842649000000000"),
				"max":       commonmodels.ParamsValue("1716842649000000000"),
				"unit":      commonmodels.ParamsValue(nanoUnit),
				"min_unit":  commonmodels.ParamsValue(nanoUnit),
				"max_unit":  commonmodels.ParamsValue(nanoUnit),
				"engine":    commonmodels.ParamsValue("random"),
				"keep_null": commonmodels.ParamsValue("true"),
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
				var val int64
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
			},
		},
		{
			name: "dynamic mode",
			staticParameters: map[string]commonmodels.ParamsValue{
				"column":   commonmodels.ParamsValue("data"),
				"unit":     commonmodels.ParamsValue(nanoUnit),
				"min_unit": commonmodels.ParamsValue(nanoUnit),
				"max_unit": commonmodels.ParamsValue(nanoUnit),
				"engine":   commonmodels.ParamsValue("random"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"min": {
					Column: "min_val",
				},
				"max": {
					Column: "max_val",
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("1234"), false),
				commonmodels.NewColumnRawValue([]byte("1616842649000000000"), false),
				commonmodels.NewColumnRawValue([]byte("1716842649000000000"), false),
			},
			columns: []commonmodels.Column{
				{
					Idx:      0,
					Name:     "data",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
				{
					Idx:      1,
					Name:     "min_val",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
				{
					Idx:      2,
					Name:     "max_val",
					TypeName: mysqldbmsdriver.TypeBigInt,
					TypeOID:  mysqldbmsdriver.VirtualOidBigInt,
					Length:   0,
					Size:     8,
				},
			},
			validateFn: func(t *testing.T, recorder commonininterfaces.Recorder) {
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
				unixTimestampTransformerDefinition,
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
