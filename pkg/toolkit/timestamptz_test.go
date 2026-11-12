// Copyright 2026 Greenmask
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

package toolkit

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tstzTestValue = "2024-01-12 23:30:00+00"

var tstzTestInstant = time.Date(2024, 1, 12, 23, 30, 0, 0, time.UTC)

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	return loc
}

// setLocal replaces time.Local for the test, so that host-dependent decoding shows up on any host.
func setLocal(t *testing.T, loc *time.Location) {
	t.Helper()
	old := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = old })
}

func assertTimeIn(t *testing.T, expected time.Time, loc *time.Location, actual any) {
	t.Helper()
	var v time.Time
	switch a := actual.(type) {
	case time.Time:
		v = a
	case pgtype.Timestamptz:
		v = a.Time
	default:
		require.Failf(t, "unexpected type", "%T", actual)
	}
	assert.Equal(t, loc, v.Location())
	assert.True(t, expected.Equal(v), "expected %s, got %s", expected, v)
}

func newTstzTestDriver(t *testing.T, customTypes []*Type, opts ...DriverOption) *Driver {
	t.Helper()
	table := &Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*Column{
			{Name: "ts", TypeName: "timestamptz", TypeOid: pgtype.TimestamptzOID, Num: 1, Length: -1},
		},
	}
	driver, warns, err := NewDriver(table, customTypes, opts...)
	require.NoError(t, err)
	require.Empty(t, warns)
	return driver
}

func TestRegisterTimestamptzLocation(t *testing.T) {
	setLocal(t, mustLoadLocation(t, "America/New_York"))
	berlin := mustLoadLocation(t, "Europe/Berlin")
	end := time.Date(2024, 1, 13, 0, 0, 0, 0, time.UTC)

	for _, loc := range []*time.Location{time.UTC, berlin} {
		t.Run(loc.String(), func(t *testing.T) {
			m := pgtype.NewMap()
			registerTimestamptzLocation(m, loc)

			var scanned time.Time
			require.NoError(t, m.Scan(pgtype.TimestamptzOID, pgtype.TextFormatCode, []byte(tstzTestValue), &scanned))
			assertTimeIn(t, tstzTestInstant, loc, scanned)

			var scannedTstz pgtype.Timestamptz
			require.NoError(t, m.Scan(pgtype.TimestamptzOID, pgtype.TextFormatCode, []byte(tstzTestValue), &scannedTstz))
			assertTimeIn(t, tstzTestInstant, loc, scannedTstz)

			typ, ok := m.TypeForOID(pgtype.TimestamptzOID)
			require.True(t, ok)
			decoded, err := typ.Codec.DecodeValue(m, pgtype.TimestamptzOID, pgtype.TextFormatCode, []byte(tstzTestValue))
			require.NoError(t, err)
			assertTimeIn(t, tstzTestInstant, loc, decoded)

			var arr []time.Time
			require.NoError(t, m.Scan(
				pgtype.TimestamptzArrayOID, pgtype.TextFormatCode, []byte(`{"2024-01-12 23:30:00+00"}`), &arr,
			))
			require.Len(t, arr, 1)
			assertTimeIn(t, tstzTestInstant, loc, arr[0])

			rangeSrc := `["2024-01-12 23:30:00+00","2024-01-13 00:00:00+00")`
			var rng pgtype.Range[time.Time]
			require.NoError(t, m.Scan(pgtype.TstzrangeOID, pgtype.TextFormatCode, []byte(rangeSrc), &rng))
			assertTimeIn(t, tstzTestInstant, loc, rng.Lower)
			assertTimeIn(t, end, loc, rng.Upper)

			var rngArr []pgtype.Range[time.Time]
			require.NoError(t, m.Scan(
				pgtype.TstzrangeArrayOID, pgtype.TextFormatCode, []byte(`{"[\"2024-01-12 23:30:00+00\",\"2024-01-13 00:00:00+00\")"}`), &rngArr,
			))
			require.Len(t, rngArr, 1)
			assertTimeIn(t, tstzTestInstant, loc, rngArr[0].Lower)

			var multi pgtype.Multirange[pgtype.Range[time.Time]]
			require.NoError(t, m.Scan(pgtype.TstzmultirangeOID, pgtype.TextFormatCode, []byte("{"+rangeSrc+"}"), &multi))
			require.Len(t, multi, 1)
			assertTimeIn(t, tstzTestInstant, loc, multi[0].Lower)

			var null *time.Time
			require.NoError(t, m.Scan(pgtype.TimestamptzOID, pgtype.TextFormatCode, nil, &null))
			assert.Nil(t, null)

			var inf pgtype.Timestamptz
			require.NoError(t, m.Scan(pgtype.TimestamptzOID, pgtype.TextFormatCode, []byte("infinity"), &inf))
			assert.Equal(t, pgtype.Infinity, inf.InfinityModifier)

			enc, err := m.Encode(pgtype.TimestamptzOID, pgtype.TextFormatCode, scanned, nil)
			require.NoError(t, err)
			assert.Equal(t, "2024-01-12 23:30:00Z", string(enc))
		})
	}

	t.Run("default map is not mutated", func(t *testing.T) {
		var v time.Time
		require.NoError(t, pgtype.NewMap().Scan(
			pgtype.TimestamptzOID, pgtype.TextFormatCode, []byte(tstzTestValue), &v,
		))
		assert.Equal(t, time.Local, v.Location())
	})
}

func TestNewDriver_timestamptz_location(t *testing.T) {
	setLocal(t, mustLoadLocation(t, "America/New_York"))
	berlin := mustLoadLocation(t, "Europe/Berlin")
	const domainOid = 99999
	domain := &Type{Oid: domainOid, Kind: 'd', BaseType: Oid(pgtype.TimestamptzOID), Name: "my_ts", Schema: "public"}

	tests := []struct {
		name     string
		opts     []DriverOption
		expected *time.Location
	}{
		{name: "no option", expected: time.UTC},
		{name: "nil location", opts: []DriverOption{WithTimestamptzLocation(nil)}, expected: time.UTC},
		{name: "Europe/Berlin", opts: []DriverOption{WithTimestamptzLocation(berlin)}, expected: berlin},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := newTstzTestDriver(t, []*Type{domain}, tt.opts...)
			assert.Equal(t, tt.expected, driver.TimestamptzLocation())

			decoded, err := driver.DecodeValueByColumnIdx(0, []byte(tstzTestValue))
			require.NoError(t, err)
			assertTimeIn(t, tstzTestInstant, tt.expected, decoded)

			var scanned time.Time
			require.NoError(t, driver.ScanValueByColumnIdx(0, []byte(tstzTestValue), &scanned))
			assertTimeIn(t, tstzTestInstant, tt.expected, scanned)

			decoded, err = driver.DecodeValueByTypeName("timestamptz", []byte(tstzTestValue))
			require.NoError(t, err)
			assertTimeIn(t, tstzTestInstant, tt.expected, decoded)

			decoded, err = driver.DecodeValueByTypeOid(domainOid, []byte(tstzTestValue))
			require.NoError(t, err)
			assertTimeIn(t, tstzTestInstant, tt.expected, decoded)
		})
	}
}

func TestTruncateDate_timestamptz_location(t *testing.T) {
	setLocal(t, mustLoadLocation(t, "America/New_York"))
	berlin := mustLoadLocation(t, "Europe/Berlin")

	tests := []struct {
		name     string
		opts     []DriverOption
		expected time.Time
	}{
		{name: "default UTC", expected: time.Date(2024, 1, 12, 0, 0, 0, 0, time.UTC)},
		{
			name:     "Europe/Berlin",
			opts:     []DriverOption{WithTimestamptzLocation(berlin)},
			expected: time.Date(2024, 1, 12, 23, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := newTstzTestDriver(t, nil, tt.opts...)
			var v time.Time
			require.NoError(t, driver.ScanValueByColumnIdx(0, []byte(tstzTestValue), &v))
			part := "day"
			res, err := TruncateDate(&part, &v)
			require.NoError(t, err)
			assert.True(t, tt.expected.Equal(*res), "expected %s, got %s", tt.expected, res.UTC())
		})
	}
}

func TestMeta_TimestamptzLocation(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
		wantErr  bool
	}{
		{name: "missing field from older greenmask", json: `{}`, expected: "UTC"},
		{name: "empty", json: `{"timezone":""}`, expected: "UTC"},
		{name: "Europe/Berlin", json: `{"timezone":"Europe/Berlin"}`, expected: "Europe/Berlin"},
		{name: "invalid", json: `{"timezone":"Mars/Olympus"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &Meta{}
			require.NoError(t, json.Unmarshal([]byte(tt.json), meta))
			loc, err := meta.TimestamptzLocation()
			if tt.wantErr {
				require.ErrorContains(t, err, "invalid timezone in meta")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, loc.String())

			meta.Table = &Table{
				Schema: "public", Name: "test", Oid: 1224,
				Columns: []*Column{{Name: "ts", TypeName: "timestamptz", TypeOid: pgtype.TimestamptzOID, Num: 1, Length: -1}},
			}
			driver, _, err := newDriverFromMeta(meta)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, driver.TimestamptzLocation().String())
		})
	}
}
