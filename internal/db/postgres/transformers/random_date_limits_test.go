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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRandomDateTransformer_independent_of_local_time_zone(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	origLocal := time.Local
	time.Local = loc
	defer func() { time.Local = origLocal }()

	values := transformRows(t, "RandomDate", "date_ts", rawValues("date_ts", "2020-01-01 00:00:00"),
		paramValues("min", "2020-01-01 00:00:00", "max", "2020-01-01 00:00:10"), nil, 10)
	for _, v := range values {
		require.Regexp(t, `^2020-01-01 00:00:(0\d|10)`, v)
	}

	values = transformRows(t, "RandomDate", "date_tstz", rawValues("date_tstz", "2020-01-01 00:00:00Z"),
		paramValues("min", "2020-01-01 03:00:00Z", "max", "2020-01-01 04:00:00Z", "truncate", "day"), nil, 5)
	for _, v := range values {
		require.Equal(t, "2020-01-01 00:00:00Z", v)
	}
}

func TestRandomDateTransformer_nanoseconds_within_limits(t *testing.T) {
	values := transformRows(t, "RandomDate", "date_ts", rawValues("date_ts", "2020-01-01 00:00:00"),
		paramValues("min", "2020-01-01 00:00:00.9", "max", "2020-01-01 00:00:01"), nil, 50)
	for _, v := range values {
		require.Regexp(t, `^2020-01-01 00:00:(00\.9\d*|01)$`, v)
	}
}

func TestRandomDateTransformer_truncate_millisecond(t *testing.T) {
	values := transformRows(t, "RandomDate", "date_ts", rawValues("date_ts", "2020-01-01 00:00:00"),
		paramValues("min", "2020-01-01 00:00:00", "max", "2020-01-01 00:00:10", "truncate", "millisecond"), nil, 20)
	for _, v := range values {
		require.Regexp(t, `^2020-01-01 00:00:\d{2}(\.\d{1,3})?$`, v)
	}
}

func TestRandomDateTransformer_dynamic_min_compatible_types(t *testing.T) {
	for _, param := range []string{"min", "max"} {
		t.Run(param, func(t *testing.T) {
			params := paramValues("min", "2020-01-01 00:00:00", "max", "2020-01-02 00:00:00")
			delete(params, param)
			_, _, warns := newTestTransformer(t, "RandomDate", "date_ts",
				rawValues("date_ts", "2020-01-01 00:00:00", "date_tstz", "2020-01-01 00:00:00Z"),
				params, map[string]*toolkit.DynamicParamValue{param: {Column: "date_tstz"}})
			require.Empty(t, warns)
		})
	}
}

func TestRandomUnixTimestampTransformer_units(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]toolkit.ParamsValue
		min    int64
		max    int64
	}{
		{
			name: "nanoseconds",
			params: paramValues("min", "1700000000999999999", "max", "1700000001999999999",
				"min_unit", "nanosecond", "max_unit", "nanosecond", "unit", "nanosecond"),
			min: 1700000000999999999,
			max: 1700000001999999999,
		},
		{
			name: "microseconds",
			params: paramValues("min", "1700000000000000", "max", "1700000010000000",
				"min_unit", "microsecond", "max_unit", "microsecond", "unit", "microsecond"),
			min: 1700000000000000,
			max: 1700000010000000,
		},
		{
			name:   "different min and max units",
			params: paramValues("min", "1700000000", "max", "1700000010000", "max_unit", "millisecond"),
			min:    1700000000,
			max:    1700000010,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := transformRows(t, "RandomUnixTimestamp", "id8", rawValues("id8", "1"), tt.params, nil, 20)
			for _, v := range values {
				i, err := strconv.ParseInt(v, 10, 64)
				require.NoError(t, err)
				require.GreaterOrEqual(t, i, tt.min)
				require.LessOrEqual(t, i, tt.max)
			}
		})
	}
}
