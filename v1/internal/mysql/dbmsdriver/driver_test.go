package dbmsdriver

import (
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
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

func TestDriver_DecodeValueByTypeOid(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		oid      models.VirtualOID
		input    []byte
		expected any
	}{
		// Numeric types
		{"tinyint", VirtualOidTinyInt, []byte("1"), int64(1)},
		{"smallint", VirtualOidSmallInt, []byte("32767"), int64(32767)},
		{"mediumint", VirtualOidMediumInt, []byte("8388607"), int64(8388607)},
		{"int", VirtualOidInt, []byte("2147483647"), int64(2147483647)},
		{"bigint", VirtualOidBigInt, []byte("9223372036854775807"), int64(9223372036854775807)},
		{"decimal", VirtualOidDecimal, []byte("123.456"), must(decimal.NewFromString("123.456"))},
		{"numeric", VirtualOidNumeric, []byte("789.01"), must(decimal.NewFromString("789.01"))},
		{"float", VirtualOidFloat, []byte("3.14"), 3.14},
		{"double", VirtualOidDouble, []byte("2.71828"), 2.71828},
		{"real", VirtualOidReal, []byte("1.618"), 1.618},
		{"bit", VirtualOidBit, []byte("1"), int64(1)},

		// Date and time
		{"date", VirtualOidDate, []byte("2024-01-01"), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"datetime", VirtualOidDateTime, []byte("2024-01-01 12:30:45"), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"timestamp", VirtualOidTimestamp, []byte("2024-01-01 12:30:45"), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"time", VirtualOidTime, []byte("12:30:45"), time.Duration(45045000000000)},
		{"year", VirtualOidYear, []byte("2024"), int64(2024)},

		// String types
		{"char", VirtualOidChar, []byte("a"), "a"},
		{"varchar", VirtualOidVarChar, []byte("abc"), "abc"},

		// Boolean
		{"boolean true", VirtualOidBoolean, []byte("1"), true},
		{"boolean false", VirtualOidBoolean, []byte("0"), false},

		// Text types
		{"tinytext", VirtualOidTinyText, []byte("tiny"), "tiny"},
		{"text", VirtualOidText, []byte("hello"), "hello"},
		{"mediumtext", VirtualOidMediumText, []byte("medium"), "medium"},
		{"longtext", VirtualOidLongText, []byte("long"), "long"},

		// Binary types
		{"binary", VirtualOidBinary, []byte{0x01, 0x02}, []byte{0x01, 0x02}},
		{"varbinary", VirtualOidVarBinary, []byte{0x03}, []byte{0x03}},

		// Blob types
		{"tinyblob", VirtualOidTinyBlob, []byte("tiny"), []byte("tiny")},
		{"blob", VirtualOidBlob, []byte("blob"), []byte("blob")},
		{"mediumblob", VirtualOidMediumBlob, []byte("medium"), []byte("medium")},
		{"longblob", VirtualOidLongBlob, []byte("long"), []byte("long")},

		// Special types
		{"enum", VirtualOidEnum, []byte("active"), "active"},
		{"set", VirtualOidSet, []byte("a,b"), "a,b"},
		// {"json", VirtualOidJSON, []byte(`{"key":"val"}`), `{"key":"val"}`}, // Uncomment if supported

		// Geometry
		{"geometry", VirtualOidGeometry, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := driver.DecodeValueByTypeOid(tc.oid, tc.input)
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

func TestDriver_EncodeValueByTypeOid(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		oid      models.VirtualOID
		input    any
		expected []byte
	}{
		// Numeric types
		{"tinyint", VirtualOidTinyInt, int64(1), []byte("1")},
		{"smallint", VirtualOidSmallInt, int64(32767), []byte("32767")},
		{"mediumint", VirtualOidMediumInt, int64(8388607), []byte("8388607")},
		{"int", VirtualOidInt, int64(2147483647), []byte("2147483647")},
		{"bigint", VirtualOidBigInt, int64(9223372036854775807), []byte("9223372036854775807")},
		{"decimal", VirtualOidDecimal, "123.456", []byte("123.456")},
		{"numeric", VirtualOidNumeric, "789.01", []byte("789.01")},
		{"float", VirtualOidFloat, float64(3.14), []byte("3.14")},
		{"double", VirtualOidDouble, float64(2.71828), []byte("2.71828")},
		{"real", VirtualOidReal, float64(1.618), []byte("1.618")},
		{"bit", VirtualOidBit, int64(1), []byte("1")},

		// Date and time
		{"date", VirtualOidDate, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), []byte("2024-01-01")},
		{"datetime", VirtualOidDateTime, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC), []byte("2024-01-01 12:30:45")},
		{"timestamp", VirtualOidTimestamp, time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC), []byte("2024-01-01 12:30:45")},
		{"time", VirtualOidTime, int64(45045000000000), []byte("12:30:45")},
		{"year", VirtualOidYear, int64(2024), []byte("2024")},

		// String types
		{"char", VirtualOidChar, "a", []byte("a")},
		{"varchar", VirtualOidVarChar, "abc", []byte("abc")},

		// Boolean
		{"boolean true", VirtualOidBoolean, true, []byte("1")},
		{"boolean false", VirtualOidBoolean, false, []byte("0")},

		// Text types
		{"tinytext", VirtualOidTinyText, "tiny", []byte("tiny")},
		{"text", VirtualOidText, "hello", []byte("hello")},
		{"mediumtext", VirtualOidMediumText, "medium", []byte("medium")},
		{"longtext", VirtualOidLongText, "long", []byte("long")},

		// Binary types
		{"binary", VirtualOidBinary, []byte{0x01, 0x02}, []byte{0x01, 0x02}},
		{"varbinary", VirtualOidVarBinary, []byte{0x03}, []byte{0x03}},

		// Blob types
		{"tinyblob", VirtualOidTinyBlob, []byte("tiny"), []byte("tiny")},
		{"blob", VirtualOidBlob, []byte("blob"), []byte("blob")},
		{"mediumblob", VirtualOidMediumBlob, []byte("medium"), []byte("medium")},
		{"longblob", VirtualOidLongBlob, []byte("long"), []byte("long")},

		// Special types
		{"enum", VirtualOidEnum, "active", []byte("active")},
		{"set", VirtualOidSet, "a,b", []byte("a,b")},
		{"json", VirtualOidJSON, `{"key":"val"}`, []byte(`{"key":"val"}`)},

		// Geometry (pass-through)
		{"geometry", VirtualOidGeometry, []byte{0x01, 0x02, 0x03}, []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := driver.EncodeValueByTypeOid(tc.oid, tc.input, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, out)
		})
	}
}

func TestDriver_ScanValueByTypeOid(t *testing.T) {
	driver := New().WithLocation(time.UTC)

	tests := []struct {
		name     string
		oid      models.VirtualOID
		input    []byte
		dest     any
		expected any
	}{
		// Integer types
		{"tinyint to int64", VirtualOidTinyInt, []byte("1"), new(int64), int64(1)},
		{"smallint to int64", VirtualOidSmallInt, []byte("32767"), new(int64), int64(32767)},
		{"mediumint to int64", VirtualOidMediumInt, []byte("8388607"), new(int64), int64(8388607)},
		{"int to int64", VirtualOidInt, []byte("2147483647"), new(int64), int64(2147483647)},
		{"bigint to int64", VirtualOidBigInt, []byte("9223372036854775807"), new(int64), int64(9223372036854775807)},
		{"year to int64", VirtualOidYear, []byte("2024"), new(int64), int64(2024)},
		{"bit to int64", VirtualOidBit, []byte("1"), new(int64), int64(1)},

		// Float/Decimal
		{"float to float64", VirtualOidFloat, []byte("3.14"), new(float64), float64(3.14)},
		{"double to float64", VirtualOidDouble, []byte("2.71828"), new(float64), float64(2.71828)},
		{"real to float64", VirtualOidReal, []byte("1.618"), new(float64), float64(1.618)},
		{"decimal to float64", VirtualOidDecimal, []byte("123.456"), new(float64), float64(123.456)},
		{"numeric to float64", VirtualOidNumeric, []byte("789.01"), new(float64), float64(789.01)},
		{"decimal to float32", VirtualOidDecimal, []byte("123.456"), new(float32), float32(123.456)},
		{"numeric to float32", VirtualOidNumeric, []byte("789.01"), new(float32), float32(789.01)},
		{"decimal to string", VirtualOidDecimal, []byte("123.456"), new(string), "123.456"},
		{"numeric to string", VirtualOidNumeric, []byte("789.01"), new(string), "789.01"},
		{"decimal to decimal", VirtualOidDecimal, []byte("123.456"), new(decimal.Decimal), must(decimal.NewFromString("123.456"))},
		{"numeric to decimal", VirtualOidNumeric, []byte("789.01"), new(decimal.Decimal), must(decimal.NewFromString("789.01"))},

		// Boolean
		{"bool true", VirtualOidBoolean, []byte("1"), new(bool), true},
		{"bool false", VirtualOidBoolean, []byte("0"), new(bool), false},

		// String types
		{"char", VirtualOidChar, []byte("c"), new(string), "c"},
		{"varchar", VirtualOidVarChar, []byte("var"), new(string), "var"},

		// Text types
		{"tinytext", VirtualOidTinyText, []byte("tiny"), new(string), "tiny"},
		{"text", VirtualOidText, []byte("text"), new(string), "text"},
		{"mediumtext", VirtualOidMediumText, []byte("medium"), new(string), "medium"},
		{"longtext", VirtualOidLongText, []byte("long"), new(string), "long"},

		// Date/Time types
		{"date", VirtualOidDate, []byte("2024-01-01"), new(time.Time), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"datetime", VirtualOidDateTime, []byte("2024-01-01 12:30:45"), new(time.Time), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"timestamp", VirtualOidTimestamp, []byte("2024-01-01 12:30:45"), new(time.Time), time.Date(2024, 1, 1, 12, 30, 45, 0, time.UTC)},
		{"time", VirtualOidTime, []byte("12:30:45"), new(time.Duration), 12*time.Hour + 30*time.Minute + 45*time.Second},

		// Binary types
		{"binary", VirtualOidBinary, []byte{0x01, 0x02}, new([]byte), []byte{0x01, 0x02}},
		{"varbinary", VirtualOidVarBinary, []byte{0x03, 0x04}, new([]byte), []byte{0x03, 0x04}},

		// Blob types
		{"tinyblob", VirtualOidTinyBlob, []byte("tiny"), new([]byte), []byte("tiny")},
		{"blob", VirtualOidBlob, []byte("blob"), new([]byte), []byte("blob")},
		{"mediumblob", VirtualOidMediumBlob, []byte("medium"), new([]byte), []byte("medium")},
		{"longblob", VirtualOidLongBlob, []byte("long"), new([]byte), []byte("long")},

		// Special string types
		{"enum", VirtualOidEnum, []byte("enumval"), new(string), "enumval"},
		{"set", VirtualOidSet, []byte("a,b"), new(string), "a,b"},

		// JSON
		{"json", VirtualOidJSON, []byte(`{"key":"val"}`), new(string), `{"key":"val"}`},

		// Geometry
		{"geometry", VirtualOidGeometry, []byte{0x01, 0x02, 0x03}, new([]byte), []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := driver.ScanValueByTypeOid(tc.oid, tc.input, tc.dest)
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
