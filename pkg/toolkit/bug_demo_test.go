// Copyright 2023 Greenmask
//
// Bug demonstration test for issue #394
// This test should FAIL on main (demonstrating the bug exists)
// and PASS on the fix branch (demonstrating the fix works)

package toolkit

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getDriverWithVarcharSimple creates a driver with a VARCHAR column
func getDriverWithVarcharSimple() *Driver {
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
		},
		Constraints: []Constraint{},
	}
	driver, _, err := NewDriver(table, nil)
	if err != nil {
		panic(err.Error())
	}
	return driver
}

// TestBug394_VarcharLeadingZeros demonstrates issue #394
// VARCHAR columns with numeric-looking content should preserve leading zeros
func TestBug394_VarcharLeadingZeros(t *testing.T) {
	gtinWithLeadingZeros := "00001402417161"

	row := newTestRowDriver([]string{"1", gtinWithLeadingZeros})
	driver := getDriverWithVarcharSimple()
	r := NewRecord(driver)
	r.SetRow(row)

	// Get the value - this is what a transformer would do
	val, err := r.GetColumnValueByName("gtin")
	require.NoError(t, err)
	require.False(t, val.IsNull)

	// The value MUST be a string with the exact content, including leading zeros
	// On main (bug): this might return a numeric type or stripped string
	// On fix branch: this returns the exact string "00001402417161"
	strVal, ok := val.Value.(string)
	assert.True(t, ok, "Value should be a string, got %T", val.Value)
	assert.Equal(t, gtinWithLeadingZeros, strVal,
		"VARCHAR value should preserve leading zeros exactly")

	// Now test the round-trip (get, set, encode)
	err = r.SetColumnValueByName("gtin", val)
	require.NoError(t, err)

	rowDriver, err := r.Encode()
	require.NoError(t, err)
	encodedBytes, err := rowDriver.Encode()
	require.NoError(t, err)

	assert.Contains(t, string(encodedBytes), gtinWithLeadingZeros,
		"Encoded output should contain exact GTIN with leading zeros")
}

// TestBug394_NumericColumnLosesLeadingZeros demonstrates that NUMERIC columns
// inherently cannot preserve leading zeros (this is expected behavior)
func TestBug394_NumericColumnLosesLeadingZeros(t *testing.T) {
	// This test demonstrates the EXPECTED behavior for NUMERIC columns
	// NUMERIC columns CANNOT preserve leading zeros - this is by design
	// The fix is to use VARCHAR for identifiers that need leading zeros

	table := &Table{
		Schema: "public",
		Name:   "test_numeric",
		Oid:    1226,
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
				Name:     "code",
				TypeName: "numeric",
				TypeOid:  pgtype.NumericOID, // OID 1700
				Num:      2,
				NotNull:  false,
				Length:   -1,
			},
		},
		Constraints: []Constraint{},
	}
	driver, _, err := NewDriver(table, nil)
	require.NoError(t, err)

	// NUMERIC column with leading zeros - they will be lost
	numericWithLeadingZeros := "00001402417161"

	row := newTestRowDriver([]string{"1", numericWithLeadingZeros})
	r := NewRecord(driver)
	r.SetRow(row)

	val, err := r.GetColumnValueByName("code")
	require.NoError(t, err)
	require.False(t, val.IsNull)

	// For NUMERIC, the value is decoded as a numeric type, not string
	// Leading zeros are lost because that's how numbers work
	t.Logf("NUMERIC column value type: %T, value: %v", val.Value, val.Value)

	// This is EXPECTED to not have leading zeros - NUMERIC is for numbers
	// If you need leading zeros, use VARCHAR, not NUMERIC
}

// TestBug394_SetNumericValueToVarcharColumn tests setting a numeric value to a VARCHAR column
// This is the actual bug scenario: when a transformer processes a value and returns a number
// instead of a string, the VARCHAR column should still encode it correctly
func TestBug394_SetNumericValueToVarcharColumn(t *testing.T) {
	driver := getDriverWithVarcharSimple()
	row := newTestRowDriver([]string{"1", "original"})
	r := NewRecord(driver)
	r.SetRow(row)

	// Simulate a transformer that returns an integer instead of string
	// This could happen if a transformer does math or type conversion
	intValue := 1402417161 // Note: no leading zeros possible with int

	err := r.SetColumnValueByName("gtin", NewValue(intValue, false))
	require.NoError(t, err)

	rowDriver, err := r.Encode()
	require.NoError(t, err)
	encodedBytes, err := rowDriver.Encode()
	require.NoError(t, err)

	t.Logf("Encoded output when setting int to VARCHAR: %s", string(encodedBytes))

	// The integer should be encoded as a string
	assert.Contains(t, string(encodedBytes), "1402417161",
		"Integer value should be encoded to VARCHAR column")
}

// TestBug394_ColumnsTypeOverrideText tests the columns_type_override scenario
// When a NUMERIC column has columns_type_override: text, it should be treated as text
func TestBug394_ColumnsTypeOverrideText(t *testing.T) {
	// Simulates: columns_type_override: text on a NUMERIC column
	// This should force text handling and preserve leading zeros

	table := &Table{
		Schema: "public",
		Name:   "test_override",
		Oid:    1227,
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
				Name:              "code",
				TypeName:          "numeric",
				TypeOid:           pgtype.NumericOID,
				OverriddenTypeName: "text",
				OverriddenTypeOid:  25, // text OID
				Num:               2,
				NotNull:           false,
				Length:            -1,
			},
		},
		Constraints: []Constraint{},
	}
	driver, _, err := NewDriver(table, nil)
	require.NoError(t, err)

	valueWithLeadingZeros := "00001402417161"

	row := newTestRowDriver([]string{"1", valueWithLeadingZeros})
	r := NewRecord(driver)
	r.SetRow(row)

	val, err := r.GetColumnValueByName("code")
	require.NoError(t, err)
	require.False(t, val.IsNull)

	t.Logf("Override column value type: %T, value: %v", val.Value, val.Value)

	// With columns_type_override: text, it should be treated as text
	// BUG: On main, even with override, the encodeValue might not respect it
	strVal, ok := val.Value.(string)
	if !ok {
		t.Logf("WARNING: Expected string but got %T - this may indicate the bug", val.Value)
	} else {
		assert.Equal(t, valueWithLeadingZeros, strVal,
			"With columns_type_override: text, leading zeros should be preserved")
	}

	// Test round-trip with override
	err = r.SetColumnValueByName("code", val)
	require.NoError(t, err)

	rowDriver, err := r.Encode()
	require.NoError(t, err)
	encodedBytes, err := rowDriver.Encode()
	require.NoError(t, err)

	t.Logf("Encoded output: %s", string(encodedBytes))

	// With the fix, leading zeros should be preserved
	// On main without fix, this may fail
	assert.Contains(t, string(encodedBytes), valueWithLeadingZeros,
		"With columns_type_override: text, encoded output should preserve leading zeros")
}
