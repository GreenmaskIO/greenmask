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

// getDriverWithVarchar creates a driver with a VARCHAR column for testing
// text content preservation (e.g., leading zeros in numeric-looking strings)
func getDriverWithVarchar() *Driver {
	table := &Table{
		Schema: "public",
		Name:   "test_varchar",
		Oid:    1225,
		Columns: []*Column{
			{
				Name:     "id",
				TypeName: "int4",
				TypeOid:  pgtype.Int4OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "gtin",
				TypeName: "varchar",
				TypeOid:  pgtype.VarcharOID, // OID 1043
				Num:      2,
				NotNull:  false,
				Length:   14,
			},
			{
				Name:     "description",
				TypeName: "text",
				TypeOid:  pgtype.TextOID, // OID 25
				Num:      3,
				NotNull:  false,
				Length:   -1,
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

// TestRecord_GetColumnValue_VarcharWithLeadingZeros tests that VARCHAR columns
// containing numeric-looking strings preserve leading zeros when retrieved.
// This is the core fix for issue #394.
func TestRecord_GetColumnValue_VarcharWithLeadingZeros(t *testing.T) {
	// GTIN with leading zeros - this is a common real-world case
	gtinWithLeadingZeros := "00001402417161"

	row := newTestRowDriver([]string{"1", gtinWithLeadingZeros, "Test product"})
	driver := getDriverWithVarchar()
	r := NewRecord(driver)
	r.SetRow(row)

	// Get the value - should preserve leading zeros
	res, err := r.GetColumnValueByName("gtin")
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	assert.Equal(t, gtinWithLeadingZeros, res.Value, "VARCHAR column should preserve leading zeros")
}

// TestRecord_GetColumnValue_TextWithLeadingZeros tests that TEXT columns
// also preserve leading zeros in numeric-looking strings.
func TestRecord_GetColumnValue_TextWithLeadingZeros(t *testing.T) {
	numericLookingString := "000123456"

	row := newTestRowDriver([]string{"1", "", numericLookingString})
	driver := getDriverWithVarchar()
	r := NewRecord(driver)
	r.SetRow(row)

	res, err := r.GetColumnValueByName("description")
	require.NoError(t, err)
	assert.False(t, res.IsNull)
	assert.Equal(t, numericLookingString, res.Value, "TEXT column should preserve leading zeros")
}

// TestRecord_SetColumnValue_VarcharPreservesLeadingZeros tests that setting
// a string value on a VARCHAR column and then encoding preserves leading zeros.
func TestRecord_SetColumnValue_VarcharPreservesLeadingZeros(t *testing.T) {
	gtinWithLeadingZeros := "00001402417161"

	row := newTestRowDriver([]string{"1", "old_value", ""})
	driver := getDriverWithVarchar()
	r := NewRecord(driver)
	r.SetRow(row)

	// Set new value with leading zeros
	err := r.SetColumnValueByName("gtin", NewValue(gtinWithLeadingZeros, false))
	require.NoError(t, err)

	// Encode and verify
	rowDriver, err := r.Encode()
	require.NoError(t, err)
	res, err := rowDriver.Encode()
	require.NoError(t, err)

	// The encoded result should contain the GTIN with leading zeros
	assert.Contains(t, string(res), gtinWithLeadingZeros, "Encoded output should preserve leading zeros")
}

// TestRecord_RoundTrip_VarcharWithLeadingZeros tests the full round-trip:
// read value, set it back, encode - should preserve exact content.
func TestRecord_RoundTrip_VarcharWithLeadingZeros(t *testing.T) {
	gtinWithLeadingZeros := "00001402417161"

	row := newTestRowDriver([]string{"1", gtinWithLeadingZeros, ""})
	driver := getDriverWithVarchar()
	r := NewRecord(driver)
	r.SetRow(row)

	// Get the value
	val, err := r.GetColumnValueByName("gtin")
	require.NoError(t, err)
	assert.Equal(t, gtinWithLeadingZeros, val.Value)

	// Set it back (simulating a pass-through transformer)
	err = r.SetColumnValueByName("gtin", val)
	require.NoError(t, err)

	// Encode
	rowDriver, err := r.Encode()
	require.NoError(t, err)
	res, err := rowDriver.Encode()
	require.NoError(t, err)

	// Verify leading zeros are preserved
	assert.Contains(t, string(res), gtinWithLeadingZeros, "Round-trip should preserve leading zeros")
}

// TestIsTextTypeOid tests the helper function that identifies text-based types.
func TestIsTextTypeOid(t *testing.T) {
	tests := []struct {
		name     string
		oid      Oid
		expected bool
	}{
		{"text", 25, true},
		{"bpchar", 1042, true},
		{"varchar", 1043, true},
		{"name", 19, true},
		{"int4", pgtype.Int4OID, false},
		{"int8", pgtype.Int8OID, false},
		{"numeric", pgtype.NumericOID, false},
		{"float4", pgtype.Float4OID, false},
		{"jsonb", pgtype.JSONBOID, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTextTypeOid(tt.oid)
			assert.Equal(t, tt.expected, result, "isTextTypeOid(%d) should be %v", tt.oid, tt.expected)
		})
	}
}

// TestBugDemo_Issue394_LeadingZerosStripped demonstrates the bug from issue #394.
//
// SCENARIO: A VARCHAR column contains a GTIN like "00001402417161". When a
// transformer reads this value via GetColumnValue and writes it back, the
// leading zeros should be preserved.
//
// BUG (before fix): The value would become "1402417161" because the old code
// would decode the value using pgx which could interpret numeric-looking
// strings as numbers, losing leading zeros.
//
// FIX: For text-typed columns (varchar, text, char, name), we now return the
// raw bytes as a string directly, preserving exact content including leading zeros.
func TestBugDemo_Issue394_LeadingZerosStripped(t *testing.T) {
	// These are real GTIN-14 values where leading zeros are mandatory
	testCases := []struct {
		name  string
		value string
	}{
		{"GTIN with 4 leading zeros", "00001402417161"},
		{"GTIN with 6 leading zeros", "00000012345678"},
		{"GTIN that is mostly zeros", "00000000000001"},
		{"Padded sequence number", "00000001"},
		{"Zero-padded code", "007"},
	}

	driver := getDriverWithVarchar()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a row with the test value in the VARCHAR column
			row := newTestRowDriver([]string{"1", tc.value, ""})
			r := NewRecord(driver)
			r.SetRow(row)

			// Step 1: Get the value (simulates what a transformer does)
			val, err := r.GetColumnValueByName("gtin")
			require.NoError(t, err)
			require.False(t, val.IsNull)

			// ASSERTION: The retrieved value must match exactly
			// Before the fix, this would fail for numeric-looking strings
			assert.Equal(t, tc.value, val.Value,
				"GetColumnValue should return exact string including leading zeros")

			// Step 2: Set it back (simulates a pass-through transformer)
			err = r.SetColumnValueByName("gtin", val)
			require.NoError(t, err)

			// Step 3: Encode the row
			rowDriver, err := r.Encode()
			require.NoError(t, err)
			encodedBytes, err := rowDriver.Encode()
			require.NoError(t, err)

			// ASSERTION: The encoded output must contain the exact value
			// Before the fix, leading zeros would be stripped here
			assert.Contains(t, string(encodedBytes), tc.value,
				"Encoded output should preserve exact string including leading zeros")
		})
	}
}

// TestBugDemo_Issue394_TextColumnAlsoAffected demonstrates that TEXT columns
// (not just VARCHAR) were also affected by the leading zeros bug.
func TestBugDemo_Issue394_TextColumnAlsoAffected(t *testing.T) {
	driver := getDriverWithVarchar()

	// Test with the TEXT column (description)
	testValue := "000123456"
	row := newTestRowDriver([]string{"1", "", testValue})
	r := NewRecord(driver)
	r.SetRow(row)

	// Get the value from TEXT column
	val, err := r.GetColumnValueByName("description")
	require.NoError(t, err)
	require.False(t, val.IsNull)

	// Must preserve leading zeros
	assert.Equal(t, testValue, val.Value,
		"TEXT column should preserve leading zeros in numeric-looking strings")
}
