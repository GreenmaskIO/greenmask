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

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestUnixTimestampTransformer_Transform__positive_cases__static(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string]toolkit.ParamsValue
		minValue   int64
		maxValue   int64
		isNull     bool
	}{
		{
			name:       "seconds",
			columnName: "id8",
			original:   "123",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1616842649"),
				"max":       toolkit.ParamsValue("1711537049"),
				"unit":      toolkit.ParamsValue(secondsUnit),
				"min_unit":  toolkit.ParamsValue(secondsUnit),
				"max_unit":  toolkit.ParamsValue(secondsUnit),
				"keep_null": toolkit.ParamsValue("true"),
			},
			minValue: 1616842649,
			maxValue: 1711537049,
		},
		{
			name:       "milliseconds",
			columnName: "id8",
			original:   "123",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1611546399134"),
				"max":       toolkit.ParamsValue("1711546399134"),
				"unit":      toolkit.ParamsValue(milliUnit),
				"min_unit":  toolkit.ParamsValue(milliUnit),
				"max_unit":  toolkit.ParamsValue(milliUnit),
				"keep_null": toolkit.ParamsValue("true"),
			},
			minValue: 1611546399134,
			maxValue: 1711546399134,
		},
		{
			name:       "milliseconds",
			columnName: "id8",
			original:   "123",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1611546399134"),
				"max":       toolkit.ParamsValue("1711546399134"),
				"unit":      toolkit.ParamsValue(microUnit),
				"min_unit":  toolkit.ParamsValue(microUnit),
				"max_unit":  toolkit.ParamsValue(microUnit),
				"keep_null": toolkit.ParamsValue("true"),
			},
			minValue: 1611546399134123,
			maxValue: 1711546399134123,
		},
		{
			name:       "nanoseconds",
			columnName: "id8",
			original:   "123",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1616842649000000000"),
				"max":       toolkit.ParamsValue("1716842649000000000"),
				"unit":      toolkit.ParamsValue(nanoUnit),
				"min_unit":  toolkit.ParamsValue(nanoUnit),
				"max_unit":  toolkit.ParamsValue(nanoUnit),
				"keep_null": toolkit.ParamsValue("true"),
				"truncate":  toolkit.ParamsValue("day"),
			},
			minValue: 1616842649000000000,
			maxValue: 1716842649000000000,
		},
		{
			name:       "nanoseconds_truncate",
			columnName: "id8",
			original:   "123",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1639692000000000000"),
				"max":       toolkit.ParamsValue("1739692000000000000"),
				"unit":      toolkit.ParamsValue(nanoUnit),
				"min_unit":  toolkit.ParamsValue(nanoUnit),
				"max_unit":  toolkit.ParamsValue(nanoUnit),
				"keep_null": toolkit.ParamsValue("true"),
				"truncate":  toolkit.ParamsValue("day"),
			},
			minValue: 1639692000000000000,
			maxValue: 1739692000000000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomUnixTimestamp")
			require.True(t, ok)

			transformerCtx, warnings, err := def.Instance(
				context.Background(),
				driver, tt.params,
				nil,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res int64
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.Equal(t, tt.isNull, isNull)

			if !isNull {
				require.GreaterOrEqual(t, res, tt.minValue)
				require.LessOrEqual(t, res, tt.maxValue)
			}

		})
	}
}

func TestUnixTimestampTransformer_Transform_null_cases(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		original   string
		params     map[string]toolkit.ParamsValue
		minValue   int64
		maxValue   int64
		isNull     bool
	}{
		{
			name:       "keep_null false and NULL seq",
			columnName: "id8",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1616842649"),
				"max":       toolkit.ParamsValue("1711537049"),
				"truncate":  toolkit.ParamsValue("month"),
				"keep_null": toolkit.ParamsValue("true"),
			},
			minValue: 1616842649,
			maxValue: 1711537049,
			isNull:   true,
		},
		{
			name:       "keep_null true and NULL seq",
			columnName: "id8",
			original:   "\\N",
			params: map[string]toolkit.ParamsValue{
				"min":       toolkit.ParamsValue("1616842649"),
				"max":       toolkit.ParamsValue("1711537049"),
				"truncate":  toolkit.ParamsValue("month"),
				"keep_null": toolkit.ParamsValue("false"),
			},
			minValue: 1616842649,
			maxValue: 1711537049,
			isNull:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			driver, record := getDriverAndRecord(tt.columnName, tt.original)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomUnixTimestamp")
			require.True(t, ok)

			transformerCtx, warnings, err := def.Instance(
				context.Background(),
				driver, tt.params,
				nil,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			r, err := transformerCtx.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res int64
			isNull, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.NoError(t, err)
			require.Equal(t, tt.isNull, isNull)

			if !isNull {
				require.GreaterOrEqual(t, res, tt.minValue)
				require.LessOrEqual(t, res, tt.maxValue)
			}

		})
	}
}

func TestUnixTimestampTransformer_Transform_dynamic(t *testing.T) {

	type expected struct {
		min int64
		max int64
	}

	tests := []struct {
		name          string
		columnName    string
		params        map[string]toolkit.ParamsValue
		dynamicParams map[string]*toolkit.DynamicParamValue
		record        map[string]*toolkit.RawValue
		expected      expected
	}{
		{
			name:       "int8",
			columnName: "id8",
			record: map[string]*toolkit.RawValue{
				"id8":      toolkit.NewRawValue([]byte("123"), false),
				"int8_val": toolkit.NewRawValue([]byte("1639692000000000000"), false),
			},
			params: map[string]toolkit.ParamsValue{
				"max":    toolkit.ParamsValue("1739692000000000000"),
				"engine": toolkit.ParamsValue("random"),
			},
			dynamicParams: map[string]*toolkit.DynamicParamValue{
				"min": {
					Column: "int8_val",
				},
			},
			expected: expected{
				min: 1639692000000000000,
				max: 1739692000000000000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			driver, record := toolkit.GetDriverAndRecord(tt.record)

			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
			def, ok := utils.DefaultTransformerRegistry.Get("RandomUnixTimestamp")
			require.True(t, ok)

			transformer, warnings, err := def.Instance(
				context.Background(),
				driver,
				tt.params,
				tt.dynamicParams,
				"",
			)
			require.NoError(t, err)
			require.Empty(t, warnings)

			err = transformer.Transformer.Init(context.Background())
			require.NoError(t, err)

			for _, dp := range transformer.DynamicParameters {
				dp.SetRecord(record)
			}

			r, err := transformer.Transformer.Transform(
				context.Background(),
				record,
			)
			require.NoError(t, err)

			var res int64
			empty, err := r.ScanColumnValueByName(tt.columnName, &res)
			require.False(t, empty)
			require.NoError(t, err)
			require.True(t, res >= tt.expected.min && res <= tt.expected.max)
		})
	}
}
