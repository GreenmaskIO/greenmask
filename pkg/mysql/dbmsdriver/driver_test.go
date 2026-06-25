// Copyright 2025 Greenmask
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

package dbmsdriver

import (
	"reflect"
	"testing"
	"time"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func must(t any, err error) any {
	if err != nil {
		panic(err)
	}
	return t
}

func TestDriver_DecodeValueByTypeName(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		typeName string
		input    []byte
		expected any
	}{
		// Numeric types
		{"tinyint", TypeTinyInt, []byte("1"), int64(1)},
		{"smallint", TypeSmallInt, []byte("32767"), int64(32767)},
		{"mediumint", TypeMediumInt, []byte("8388607"), int64(8388607)},
		{"int", TypeInt, []byte("2147483647"), int64(2147483647)},
		{"bigint", TypeBigInt, []byte("9223372036854775807"), int64(9223372036854775807)},
		{"decimal", TypeDecimal, []byte("123.456"), must(decimal.NewFromString("123.456"))},
		{"numeric", TypeNumeric, []byte("789.01"), must(decimal.NewFromString("789.01"))},
		{"float", TypeFloat, []byte("3.14"), 3.14},
		{"double", TypeDouble, []byte("2.71828"), 2.71828},
		{"real", TypeReal, []byte("1.618"), 1.618},
		{"bit", TypeBit, []byte("1"), int64(1)},

		// Date and time
		{"date", TypeDate, []byte("2024-01-01"), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"datetime", TypeDateTime, []byte("2024-01-01 12:30:45"), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"timestamp", TypeTimestamp, []byte("2024-01-01 12:30:45"), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"time", TypeTime, []byte("12:30:45"), time.Duration(45045000000000)},
		{"year", TypeYear, []byte("2024"), int64(2024)},

		// String types
		{"char", TypeChar, []byte("a"), "a"},
		{"varchar", TypeVarChar, []byte("abc"), "abc"},

		// Boolean
		{"boolean true", TypeBoolean, []byte("1"), true},
		{"boolean false", TypeBoolean, []byte("0"), false},

		// Text types
		{"tinytext", TypeTinyText, []byte("tiny"), "tiny"},
		{"text", TypeText, []byte("hello"), "hello"},
		{"mediumtext", TypeMediumText, []byte("medium"), "medium"},
		{"longtext", TypeLongText, []byte("long"), "long"},

		// Binary types
		{"binary", TypeBinary, []byte{0x01, 0x02}, []byte{0x01, 0x02}},
		{"varbinary", TypeVarBinary, []byte{0x03}, []byte{0x03}},

		// Blob types
		{"tinyblob", TypeTinyBlob, []byte("tiny"), []byte("tiny")},
		{"blob", TypeBlob, []byte("blob"), []byte("blob")},
		{"mediumblob", TypeMediumBlob, []byte("medium"), []byte("medium")},
		{"longblob", TypeLongBlob, []byte("long"), []byte("long")},

		// Special types
		{"enum", TypeEnum, []byte("active"), "active"},
		{"set", TypeSet, []byte("a,b"), "a,b"},
		//{"json", TypeJSON, []byte(`{"key":"val"}"), "{"key":"val"}`), ""},

		// Geometry placeholder
		{"geometry", TypeGeometry, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := driver.DecodeValueByTypeName(tc.typeName, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, val)
		})
	}
}

func TestDriver_DecodeValueByTypeID(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		oid      core.TypeID
		input    []byte
		expected any
	}{
		// Numeric types
		{"tinyint", TypeIDTinyInt, []byte("1"), int64(1)},
		{"smallint", TypeIDSmallInt, []byte("32767"), int64(32767)},
		{"mediumint", TypeIDMediumInt, []byte("8388607"), int64(8388607)},
		{"int", TypeIDInt, []byte("2147483647"), int64(2147483647)},
		{"bigint", TypeIDBigInt, []byte("9223372036854775807"), int64(9223372036854775807)},
		{"decimal", TypeIDDecimal, []byte("123.456"), must(decimal.NewFromString("123.456"))},
		{"numeric", TypeIDNumeric, []byte("789.01"), must(decimal.NewFromString("789.01"))},
		{"float", TypeIDFloat, []byte("3.14"), 3.14},
		{"double", TypeIDDouble, []byte("2.71828"), 2.71828},
		{"real", TypeIDReal, []byte("1.618"), 1.618},
		{"bit", TypeIDBit, []byte("1"), int64(1)},

		// Date and time
		{"date", TypeIDDate, []byte("2024-01-01"), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"datetime", TypeIDDateTime, []byte("2024-01-01 12:30:45"), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"timestamp", TypeIDTimestamp, []byte("2024-01-01 12:30:45"), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"time", TypeIDTime, []byte("12:30:45"), time.Duration(45045000000000)},
		{"year", TypeIDYear, []byte("2024"), int64(2024)},

		// String types
		{"char", TypeIDChar, []byte("a"), "a"},
		{"varchar", TypeIDVarChar, []byte("abc"), "abc"},

		// Boolean
		{"boolean true", TypeIDBoolean, []byte("1"), true},
		{"boolean false", TypeIDBoolean, []byte("0"), false},

		// Text types
		{"tinytext", TypeIDTinyText, []byte("tiny"), "tiny"},
		{"text", TypeIDText, []byte("hello"), "hello"},
		{"mediumtext", TypeIDMediumText, []byte("medium"), "medium"},
		{"longtext", TypeIDLongText, []byte("long"), "long"},

		// Binary types
		{"binary", TypeIDBinary, []byte{0x01, 0x02}, []byte{0x01, 0x02}},
		{"varbinary", TypeIDVarBinary, []byte{0x03}, []byte{0x03}},

		// Blob types
		{"tinyblob", TypeIDTinyBlob, []byte("tiny"), []byte("tiny")},
		{"blob", TypeIDBlob, []byte("blob"), []byte("blob")},
		{"mediumblob", TypeIDMediumBlob, []byte("medium"), []byte("medium")},
		{"longblob", TypeIDLongBlob, []byte("long"), []byte("long")},

		// Special types
		{"enum", TypeIDEnum, []byte("active"), "active"},
		{"set", TypeIDSet, []byte("a,b"), "a,b"},
		// {"json", TypeIDJSON, []byte(`{"key":"val"}`), `{"key":"val"}`}, // Uncomment if supported

		// Geometry
		{"geometry", TypeIDGeometry, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := driver.DecodeValueByTypeID(tc.oid, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, val)
		})
	}
}

func TestDriver_EncodeValueByTypeName(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		typeName string
		input    any
		expected []byte
	}{
		// Numeric types
		{"tinyint", TypeTinyInt, int64(1), []byte("1")},
		{"smallint", TypeSmallInt, int64(32767), []byte("32767")},
		{"mediumint", TypeMediumInt, int64(8388607), []byte("8388607")},
		{"int", TypeInt, int64(2147483647), []byte("2147483647")},
		{"bigint", TypeBigInt, int64(9223372036854775807), []byte("9223372036854775807")},
		{"decimal", TypeDecimal, "123.456", []byte("123.456")},
		{"numeric", TypeNumeric, "789.01", []byte("789.01")},
		{"float", TypeFloat, float64(3.14), []byte("3.14")},
		{"double", TypeDouble, float64(2.71828), []byte("2.71828")},
		{"real", TypeReal, float64(1.618), []byte("1.618")},
		{"bit", TypeBit, int64(1), []byte("1")},

		// Date and time
		{"date", TypeDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("2024-01-01")},
		{"datetime", TypeDateTime, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC), []byte("2024-01-01 12:30:45")},
		{"timestamp", TypeTimestamp, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC), []byte("2024-01-01 12:30:45")},
		{"time", TypeTime, int64(45045000000000), []byte("12:30:45")}, // 12h30m45s
		{"year", TypeYear, int64(2024), []byte("2024")},

		// String types
		{"char", TypeChar, "a", []byte("a")},
		{"varchar", TypeVarChar, "abc", []byte("abc")},

		// Boolean
		{"boolean true", TypeBoolean, true, []byte("1")},
		{"boolean false", TypeBoolean, false, []byte("0")},

		// Text types
		{"tinytext", TypeTinyText, "tiny", []byte("tiny")},
		{"text", TypeText, "hello", []byte("hello")},
		{"mediumtext", TypeMediumText, "medium", []byte("medium")},
		{"longtext", TypeLongText, "long", []byte("long")},

		// Binary types
		{"binary", TypeBinary, []byte{0x01, 0x02}, []byte{0x01, 0x02}},
		{"varbinary", TypeVarBinary, []byte{0x03}, []byte{0x03}},

		// Blob types
		{"tinyblob", TypeTinyBlob, []byte("tiny"), []byte("tiny")},
		{"blob", TypeBlob, []byte("blob"), []byte("blob")},
		{"mediumblob", TypeMediumBlob, []byte("medium"), []byte("medium")},
		{"longblob", TypeLongBlob, []byte("long"), []byte("long")},

		// Special types
		{"enum", TypeEnum, "active", []byte("active")},
		{"set", TypeSet, "a,b", []byte("a,b")},
		{"json", TypeJSON, `{"key":"val"}`, []byte(`{"key":"val"}`)},

		// Geometry (assume pass-through)
		{"geometry", TypeGeometry, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := driver.EncodeValueByTypeName(tc.typeName, tc.input, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, out)
		})
	}
}

func TestDriver_EncodeValueByTypeID(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		oid      core.TypeID
		input    any
		expected []byte
	}{
		// Numeric types
		{"tinyint", TypeIDTinyInt, int64(1), []byte("1")},
		{"smallint", TypeIDSmallInt, int64(32767), []byte("32767")},
		{"mediumint", TypeIDMediumInt, int64(8388607), []byte("8388607")},
		{"int", TypeIDInt, int64(2147483647), []byte("2147483647")},
		{"bigint", TypeIDBigInt, int64(9223372036854775807), []byte("9223372036854775807")},
		{"decimal", TypeIDDecimal, "123.456", []byte("123.456")},
		{"numeric", TypeIDNumeric, "789.01", []byte("789.01")},
		{"float", TypeIDFloat, float64(3.14), []byte("3.14")},
		{"double", TypeIDDouble, float64(2.71828), []byte("2.71828")},
		{"real", TypeIDReal, float64(1.618), []byte("1.618")},
		{"bit", TypeIDBit, int64(1), []byte("1")},

		// Date and time
		{"date", TypeIDDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("2024-01-01")},
		{"datetime", TypeIDDateTime, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC), []byte("2024-01-01 12:30:45")},
		{"timestamp", TypeIDTimestamp, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC), []byte("2024-01-01 12:30:45")},
		{"time", TypeIDTime, int64(45045000000000), []byte("12:30:45")},
		{"year", TypeIDYear, int64(2024), []byte("2024")},

		// String types
		{"char", TypeIDChar, "a", []byte("a")},
		{"varchar", TypeIDVarChar, "abc", []byte("abc")},

		// Boolean
		{"boolean true", TypeIDBoolean, true, []byte("1")},
		{"boolean false", TypeIDBoolean, false, []byte("0")},

		// Text types
		{"tinytext", TypeIDTinyText, "tiny", []byte("tiny")},
		{"text", TypeIDText, "hello", []byte("hello")},
		{"mediumtext", TypeIDMediumText, "medium", []byte("medium")},
		{"longtext", TypeIDLongText, "long", []byte("long")},

		// Binary types
		{"binary", TypeIDBinary, []byte{0x01, 0x02}, []byte{0x01, 0x02}},
		{"varbinary", TypeIDVarBinary, []byte{0x03}, []byte{0x03}},

		// Blob types
		{"tinyblob", TypeIDTinyBlob, []byte("tiny"), []byte("tiny")},
		{"blob", TypeIDBlob, []byte("blob"), []byte("blob")},
		{"mediumblob", TypeIDMediumBlob, []byte("medium"), []byte("medium")},
		{"longblob", TypeIDLongBlob, []byte("long"), []byte("long")},

		// Special types
		{"enum", TypeIDEnum, "active", []byte("active")},
		{"set", TypeIDSet, "a,b", []byte("a,b")},
		{"json", TypeIDJSON, `{"key":"val"}`, []byte(`{"key":"val"}`)},

		// Geometry (pass-through)
		{"geometry", TypeIDGeometry, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := driver.EncodeValueByTypeID(tc.oid, tc.input, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, out)
		})
	}
}

func TestDriver_ScanValueByTypeID(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		oid      core.TypeID
		input    []byte
		dest     any
		expected any
	}{
		// Integer types
		{"tinyint to int64", TypeIDTinyInt, []byte("1"), new(int64), int64(1)},
		{"smallint to int64", TypeIDSmallInt, []byte("32767"), new(int64), int64(32767)},
		{"mediumint to int64", TypeIDMediumInt, []byte("8388607"), new(int64), int64(8388607)},
		{"int to int64", TypeIDInt, []byte("2147483647"), new(int64), int64(2147483647)},
		{"bigint to int64", TypeIDBigInt, []byte("9223372036854775807"), new(int64), int64(9223372036854775807)},
		{"year to int64", TypeIDYear, []byte("2024"), new(int64), int64(2024)},
		{"bit to int64", TypeIDBit, []byte("1"), new(int64), int64(1)},

		// Float/Decimal
		{"float to float64", TypeIDFloat, []byte("3.14"), new(float64), float64(3.14)},
		{"double to float64", TypeIDDouble, []byte("2.71828"), new(float64), float64(2.71828)},
		{"real to float64", TypeIDReal, []byte("1.618"), new(float64), float64(1.618)},
		{"decimal to float64", TypeIDDecimal, []byte("123.456"), new(float64), float64(123.456)},
		{"numeric to float64", TypeIDNumeric, []byte("789.01"), new(float64), float64(789.01)},
		{"decimal to float32", TypeIDDecimal, []byte("123.456"), new(float32), float32(123.456)},
		{"numeric to float32", TypeIDNumeric, []byte("789.01"), new(float32), float32(789.01)},
		{"decimal to string", TypeIDDecimal, []byte("123.456"), new(string), "123.456"},
		{"numeric to string", TypeIDNumeric, []byte("789.01"), new(string), "789.01"},
		{"decimal to decimal", TypeIDDecimal, []byte("123.456"), new(decimal.Decimal), must(decimal.NewFromString("123.456"))},
		{"numeric to decimal", TypeIDNumeric, []byte("789.01"), new(decimal.Decimal), must(decimal.NewFromString("789.01"))},

		// Boolean
		{"bool true", TypeIDBoolean, []byte("1"), new(bool), true},
		{"bool false", TypeIDBoolean, []byte("0"), new(bool), false},

		// String types
		{"char", TypeIDChar, []byte("c"), new(string), "c"},
		{"varchar", TypeIDVarChar, []byte("var"), new(string), "var"},

		// Text types
		{"tinytext", TypeIDTinyText, []byte("tiny"), new(string), "tiny"},
		{"text", TypeIDText, []byte("text"), new(string), "text"},
		{"mediumtext", TypeIDMediumText, []byte("medium"), new(string), "medium"},
		{"longtext", TypeIDLongText, []byte("long"), new(string), "long"},

		// Date/Time types
		{"date", TypeIDDate, []byte("2024-01-01"), new(time.Time), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"datetime", TypeIDDateTime, []byte("2024-01-01 12:30:45"), new(time.Time), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"timestamp", TypeIDTimestamp, []byte("2024-01-01 12:30:45"), new(time.Time), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"time", TypeIDTime, []byte("12:30:45"), new(time.Duration), 12*time.Hour + 30*time.Minute + 45*time.Second},

		// Binary types
		{"binary", TypeIDBinary, []byte{0x01, 0x02}, new([]byte), []byte{0x01, 0x02}},
		{"varbinary", TypeIDVarBinary, []byte{0x03, 0x04}, new([]byte), []byte{0x03, 0x04}},

		// Blob types
		{"tinyblob", TypeIDTinyBlob, []byte("tiny"), new([]byte), []byte("tiny")},
		{"blob", TypeIDBlob, []byte("blob"), new([]byte), []byte("blob")},
		{"mediumblob", TypeIDMediumBlob, []byte("medium"), new([]byte), []byte("medium")},
		{"longblob", TypeIDLongBlob, []byte("long"), new([]byte), []byte("long")},

		// Special string types
		{"enum", TypeIDEnum, []byte("enumval"), new(string), "enumval"},
		{"set", TypeIDSet, []byte("a,b"), new(string), "a,b"},

		// JSON
		{"json", TypeIDJSON, []byte(`{"key":"val"}`), new(string), `{"key":"val"}`},

		// Geometry
		{"geometry", TypeIDGeometry, []byte{0x01, 0x02, 0x03}, new([]byte), []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := driver.ScanValueByTypeID(tc.oid, tc.input, tc.dest)
			require.NoError(t, err)

			destVal := reflect.ValueOf(tc.dest)
			if destVal.Kind() != reflect.Ptr {
				t.Fatalf("expected pointer, got %T", tc.dest)
			}
			actual := destVal.Elem().Interface()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestDriver_ScanValueByTypeName(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		typeName string
		input    []byte
		dest     any
		expected any
	}{
		// Integer types
		{"tinyint to int64", TypeTinyInt, []byte("1"), new(int64), int64(1)},
		{"smallint to int64", TypeSmallInt, []byte("32767"), new(int64), int64(32767)},
		{"mediumint to int64", TypeMediumInt, []byte("8388607"), new(int64), int64(8388607)},
		{"int to int64", TypeInt, []byte("2147483647"), new(int64), int64(2147483647)},
		{"bigint to int64", TypeBigInt, []byte("9223372036854775807"), new(int64), int64(9223372036854775807)},
		{"year to int64", TypeYear, []byte("2024"), new(int64), int64(2024)},
		{"bit to int64", TypeBit, []byte("1"), new(int64), int64(1)},

		// Float/Decimal
		{"float to float64", TypeFloat, []byte("3.14"), new(float64), float64(3.14)},
		{"double to float64", TypeDouble, []byte("2.71828"), new(float64), float64(2.71828)},
		{"real to float64", TypeReal, []byte("1.618"), new(float64), float64(1.618)},
		{"decimal to float64", TypeDecimal, []byte("123.456"), new(float64), float64(123.456)},
		{"numeric to float64", TypeNumeric, []byte("789.01"), new(float64), float64(789.01)},
		{"decimal to float32", TypeDecimal, []byte("123.456"), new(float32), float32(123.456)},
		{"numeric to float32", TypeNumeric, []byte("789.01"), new(float32), float32(789.01)},
		{"decimal to string", TypeDecimal, []byte("123.456"), new(string), "123.456"},
		{"numeric to string", TypeNumeric, []byte("789.01"), new(string), "789.01"},
		{"decimal to decimal", TypeDecimal, []byte("123.456"), new(decimal.Decimal), must(decimal.NewFromString("123.456"))},
		{"numeric to decimal", TypeNumeric, []byte("789.01"), new(decimal.Decimal), must(decimal.NewFromString("789.01"))},

		// Boolean
		{"bool true", TypeBoolean, []byte("1"), new(bool), true},
		{"bool false", TypeBoolean, []byte("0"), new(bool), false},

		// String types
		{"char", TypeChar, []byte("c"), new(string), "c"},
		{"varchar", TypeVarChar, []byte("var"), new(string), "var"},

		// Text types
		{"tinytext", TypeTinyText, []byte("tiny"), new(string), "tiny"},
		{"text", TypeText, []byte("text"), new(string), "text"},
		{"mediumtext", TypeMediumText, []byte("medium"), new(string), "medium"},
		{"longtext", TypeLongText, []byte("long"), new(string), "long"},

		// Date/Time types
		{"date", TypeDate, []byte("2024-01-01"), new(time.Time), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"datetime", TypeDateTime, []byte("2024-01-01 12:30:45"), new(time.Time), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"timestamp", TypeTimestamp, []byte("2024-01-01 12:30:45"), new(time.Time), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"time", TypeTime, []byte("12:30:45"), new(time.Duration), 12*time.Hour + 30*time.Minute + 45*time.Second},

		// Binary types
		{"binary", TypeBinary, []byte{0x01, 0x02}, new([]byte), []byte{0x01, 0x02}},
		{"varbinary", TypeVarBinary, []byte{0x03, 0x04}, new([]byte), []byte{0x03, 0x04}},

		// Blob types
		{"tinyblob", TypeTinyBlob, []byte("tiny"), new([]byte), []byte("tiny")},
		{"blob", TypeBlob, []byte("blob"), new([]byte), []byte("blob")},
		{"mediumblob", TypeMediumBlob, []byte("medium"), new([]byte), []byte("medium")},
		{"longblob", TypeLongBlob, []byte("long"), new([]byte), []byte("long")},

		// Special string types
		{"enum", TypeEnum, []byte("enumval"), new(string), "enumval"},
		{"set", TypeSet, []byte("a,b"), new(string), "a,b"},

		// JSON
		{"json", TypeJSON, []byte(`{"key":"val"}`), new(string), `{"key":"val"}`},

		// Geometry
		{"geometry", TypeGeometry, []byte{0x01, 0x02, 0x03}, new([]byte), []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := driver.ScanValueByTypeName(tc.typeName, tc.input, tc.dest)
			require.NoError(t, err)

			val := reflect.ValueOf(tc.dest)
			if val.Kind() != reflect.Ptr {
				t.Fatalf("expected pointer, got %T", tc.dest)
			}
			actual := val.Elem().Interface()
			assert.Equal(t, tc.expected, actual)
		})
	}
}
