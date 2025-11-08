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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestNoiseDateTransformer_Transform(t *testing.T) {
	loc := time.Now().Location()
	tests := []struct {
		name             string
		columnName       string
		staticParameters map[string]commonmodels.ParamsValue
		dynamicParameter map[string]commonmodels.DynamicParamValue
		original         []*commonmodels.ColumnRawValue
		validateFn       func(t *testing.T, recorder commonininterfaces.Recorder)
		expectedErr      string
		columns          []commonmodels.Column
		isNull           bool
	}{
		{
			name:       "test date type",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"max_ratio": commonmodels.ParamsValue(`
					{
						"years": 1,
						"months": 1,
						"days": 1,
						"hours": 1,
						"minutes": 1,
						"seconds": 1,
						"milliseconds": 1
					}
				`),
				"min_ratio": commonmodels.ParamsValue(`
					{
						"months": 1,
						"days": 1,
						"hours": 1,
						"minutes": 1,
						"seconds": 1,
						"milliseconds": 1
					}
				`),
				"column": commonmodels.ParamsValue("data"),
			},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: commonmodels.TypeClassDateTime,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("2023-06-25 00:00:00"), false),
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				minDate := time.Date(2022, 3, 1, 22, 00, 0, 0, loc)
				maxDate := time.Date(2024, 8, 29, 1, 1, 1, 1000, loc)
				// ^\d{4}-\d{2}-\d{2}$
				val, err := record.GetColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res, ok := val.Value.(time.Time)
				require.True(t, ok, "expected time.Time type")
				assert.WithinRangef(t, res, minDate, maxDate, "result is out of range")
			},
		},
		{
			name:       "test truncate",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"max_ratio": commonmodels.ParamsValue(`
					{
						"years": 1,
						"months": 1,
						"days": 1,
						"hours": 1,
						"minutes": 1,
						"seconds": 1,
						"milliseconds": 1
					}
				`),
				"min_ratio": commonmodels.ParamsValue(`
					{
						"months": 1,
						"days": 1,
						"hours": 1,
						"minutes": 1,
						"seconds": 1,
						"milliseconds": 1
					}
				`),
				"column":   commonmodels.ParamsValue("data"),
				"truncate": commonmodels.ParamsValue("month"),
			},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: commonmodels.TypeClassDateTime,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("2023-06-25 00:00:00"), false),
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				minDate := time.Date(2022, 3, 1, 22, 00, 0, 0, loc)
				maxDate := time.Date(2024, 8, 29, 1, 1, 1, 1000, loc)
				// ^\d{4}-\d{2}-\d{2}$
				val, err := record.GetColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res, ok := val.Value.(time.Time)
				require.True(t, ok, "expected time.Time type")
				assert.WithinRangef(t, res, minDate, maxDate, "result is out of range")
				assert.Equal(t, 1, res.Day(), "day should be truncated to 1, got %d", res.Day())
				assert.Zerof(t, res.Second(), "second should be truncated to 0, got %d", res.Second())
				assert.Zerof(t, res.Nanosecond(), "nanosecond should be truncated to 0, got %d", res.Nanosecond())
			},
		},
		{
			name:       "dynamic",
			columnName: "data",
			staticParameters: map[string]commonmodels.ParamsValue{
				"max_ratio": commonmodels.ParamsValue(`
					{
						"years": 1,
						"months": 1,
						"days": 1,
						"hours": 1,
						"minutes": 1,
						"seconds": 1,
						"milliseconds": 1
					}
				`),
				"min_ratio": commonmodels.ParamsValue(`
					{
						"months": 1,
						"days": 1,
						"hours": 1,
						"minutes": 1,
						"seconds": 1,
						"milliseconds": 1
					}
				`),
				"column":   commonmodels.ParamsValue("data"),
				"truncate": commonmodels.ParamsValue("month"),
			},
			dynamicParameter: map[string]commonmodels.DynamicParamValue{
				"min": {
					Column: "min_col",
				},
				"max": {
					Column: "max_col",
				},
			},
			columns: []commonmodels.Column{
				{
					Idx:       0,
					Name:      "data",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: commonmodels.TypeClassDateTime,
				},
				{
					Idx:       1,
					Name:      "min_col",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: commonmodels.TypeClassDateTime,
				},
				{
					Idx:       1,
					Name:      "max_col",
					TypeName:  mysqldbmsdriver.TypeTimestamp,
					TypeOID:   mysqldbmsdriver.VirtualOidTimestamp,
					TypeClass: commonmodels.TypeClassDateTime,
				},
			},
			original: []*commonmodels.ColumnRawValue{
				commonmodels.NewColumnRawValue([]byte("2023-01-01 00:00:00"), false),
				commonmodels.NewColumnRawValue([]byte("2022-11-01 00:00:00"), false),
				commonmodels.NewColumnRawValue([]byte("2023-02-01 00:00:00"), false),
			},
			validateFn: func(t *testing.T, record commonininterfaces.Recorder) {
				minDate := time.Date(2022, 11, 1, 0, 00, 0, 0, loc)
				maxDate := time.Date(2023, 2, 1, 0, 0, 0, 0, loc)
				// ^\d{4}-\d{2}-\d{2}$
				val, err := record.GetColumnValueByName("data")
				require.NoError(t, err)
				require.False(t, val.IsNull)
				res, ok := val.Value.(time.Time)
				require.True(t, ok, "expected time.Time type")
				assert.WithinRangef(t, res, minDate, maxDate, "result is out of range")
				assert.Equal(t, 1, res.Day(), "day should be truncated to 1, got %d", res.Day())
				assert.Zerof(t, res.Second(), "second should be truncated to 0, got %d", res.Second())
				assert.Zerof(t, res.Nanosecond(), "nanosecond should be truncated to 0, got %d", res.Nanosecond())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			env := newTransformerTestEnvReal(t,
				NoiseDateTransformerDefinition,
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
