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

package toolkit

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDriver() *Driver {
	table := &Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*Column{
			{
				Name:     "id",
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "created_at",
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
				Num:      2,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "title",
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
				Num:      3,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "json_data",
				TypeName: "jsonb",
				TypeOid:  pgtype.JSONBOID,
				Num:      4,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "float_data",
				TypeName: "float4",
				TypeOid:  pgtype.Float4OID,
				Num:      5,
				NotNull:  true,
				Length:   4,
			},
		},
		Constraints: []Constraint{},
	}
	driver, _, err := NewDriver(table, nil)
	if err != nil {
		panic(err.Error())
	}
	return driver
}

func TestRecord_ScanAttribute(t *testing.T) {
	row := newTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000", ""})
	driver := getDriver()
	r := NewRecord(driver)
	r.SetRow(row)
	var res int
	var expected = 1
	_, err := r.ScanColumnValueByName("id", &res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestRecord_GetAttribute_date(t *testing.T) {
	row := newTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000", ""})
	driver := getDriver()
	r := NewRecord(driver)
	r.SetRow(row)
	res, err := r.GetColumnValueByName("created_at")
	require.NoError(t, err)
	expected := NewValue(time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC), false)
	assert.Equal(t, expected.IsNull, res.IsNull)
	assert.Equal(t, expected.Value, res.Value)
}

func TestRecord_GetAttribute_text(t *testing.T) {
	row := newTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000", "1234", ""})
	driver := getDriver()
	r := NewRecord(driver)
	r.SetRow(row)
	res, err := r.GetColumnValueByName("title")
	require.NoError(t, err)
	expected := NewValue("1234", false)
	assert.Equal(t, expected.IsNull, res.IsNull)
	assert.Equal(t, expected.Value, res.Value)
}

//func TestRecord_GetTuple(t *testing.T) {
//	expected := Tuple{
//		"id":         NewValue(int16(1), false),
//		"created_at": NewValue(time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC), false),
//		"title":      NewValue(nil, true),
//	}
//	row := &TestRowDriver{
//		row: []string{"1", "2023-08-27 00:00:00.000000", testNullSeq, "", ""},
//	}
//	driver := getDriver()
//	r := NewRecord(driver)
//	r.SetRow(row)
//	res, err := r.GetTuple()
//	require.NoError(t, err)
//	for name := range expected {
//		assert.Equalf(t, expected[name].IsNull, res[name].IsNull, "wrong IsNull value %s", name)
//		assert.Equalf(t, expected[name].Value, res[name].Value, "wrong Value %s", name)
//	}
//}

func TestRecord_Encode(t *testing.T) {
	row := newTestRowDriver([]string{"1", "2023-08-27 00:00:00.000001", "test", "", ""})
	expected := []byte("2\t2023-08-29 00:00:00.000002\t\\N\t\t")
	driver := getDriver()
	r := NewRecord(driver)
	r.SetRow(row)
	err := r.SetColumnValueByName("id", NewValue(int16(2), false))
	require.NoError(t, err)
	err = r.SetColumnValueByName("created_at", NewValue(time.Date(2023, time.August, 29, 0, 0, 0, 2000, time.UTC), false))
	require.NoError(t, err)
	err = r.SetColumnValueByName("title", NewValue(nil, true))
	require.NoError(t, err)
	rowDriver, err := r.Encode()
	require.NoError(t, err)
	res, err := rowDriver.Encode()
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}
