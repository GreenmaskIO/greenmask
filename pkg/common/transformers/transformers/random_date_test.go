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
	"time"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isDateBetween(t *testing.T, from, to, value time.Time) {
	require.Truef(t, (value.Equal(from) || value.After(from)) && (value.Equal(to) || value.Before(to)),
		"date %s is not between %s and %s", value.Format(time.RFC3339), from.Format(time.RFC3339),
		to.Format(time.RFC3339),
	)
}

func TestTimestampTransformer_Transform(t *testing.T) {

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
			name: "test date type",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeDate,
					TypeOID:   mysqldbmsdriver.VirtualOidDate,
					TypeClass: core.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("2007-09-14"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"min":    core.ParamsValue("2017-09-14"),
				"max":    core.ParamsValue("2023-09-14"),
				"engine": core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val time.Time
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				from, err := time.Parse("2006-01-02", "2017-09-14")
				require.NoError(t, err)
				to, err := time.Parse("2006-01-02", "2023-09-14")
				require.NoError(t, err)
				isDateBetween(t, from, to, val)
			},
		},
		{
			name: "test timestamp without timezone type",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: core.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("2008-12-15 23:34:17.946707"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column": core.ParamsValue("data"),
				"min":    core.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":    core.ParamsValue("2023-09-14 00:00:17.946707"),
				"engine": core.ParamsValue("random"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val time.Time
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				from, err := time.Parse("2006-01-02 15:04:05.000000", "2018-12-15 23:34:17.946707")
				require.NoError(t, err)
				to, err := time.Parse("2006-01-02 15:04:05.000000", "2023-09-14 00:00:17.946707")
				require.NoError(t, err)
				isDateBetween(t, from, to, val)
			},
		},
		{
			name: "test timestamp type with Truncate till day",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: core.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue([]byte("2008-12-15 23:34:17.946707"), false)},
			staticParameters: map[string]core.ParamsValue{
				"column":   core.ParamsValue("data"),
				"min":      core.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":      core.ParamsValue("2023-09-14 00:00:17.946707"),
				"engine":   core.ParamsValue("random"),
				"truncate": core.ParamsValue("month"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val time.Time
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				from, err := time.Parse("2006-01-02 15:04:05.000000", "2018-12-15 23:34:17.946707")
				require.NoError(t, err)
				to, err := time.Parse("2006-01-02 15:04:05.000000", "2023-09-14 00:00:17.946707")
				require.NoError(t, err)
				isDateBetween(t, from, to, val)
				assert.Equal(t, 1, val.Day())
				assert.Equal(t, 0, val.Hour())
				assert.Equal(t, 0, val.Minute())
				assert.Equal(t, 0, val.Second())
			},
		},
		{
			name: "keep_null false and NULL seq",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: core.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min":       core.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":       core.ParamsValue("2023-09-14 00:00:17.946707"),
				"engine":    core.ParamsValue("random"),
				"keep_null": core.ParamsValue("false"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val time.Time
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.False(t, isNull)
				from, err := time.Parse("2006-01-02 15:04:05.000000", "2018-12-15 23:34:17.946707")
				require.NoError(t, err)
				to, err := time.Parse("2006-01-02 15:04:05.000000", "2023-09-14 00:00:17.946707")
				require.NoError(t, err)
				isDateBetween(t, from, to, val)
			},
		},
		{
			name: "keep_null true and NULL seq",
			columns: []core.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: core.TypeClassDateTime,
					Length:    0,
				},
			},
			original: []*core.ColumnRawValue{
				core.NewColumnRawValue(nil, true)},
			staticParameters: map[string]core.ParamsValue{
				"column":    core.ParamsValue("data"),
				"min":       core.ParamsValue("2018-12-15 23:34:17.946707"),
				"max":       core.ParamsValue("2023-09-14 00:00:17.946707"),
				"engine":    core.ParamsValue("random"),
				"keep_null": core.ParamsValue("true"),
			},
			validateFn: func(t *testing.T, recorder core.Recorder) {
				var val time.Time
				isNull, err := recorder.ScanColumnValueByName("data", &val)
				require.NoError(t, err)
				require.True(t, isNull)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				RandomDateTransformerDefinition,
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
