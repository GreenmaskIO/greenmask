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

package coretest

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// TestDriver_DecodeValueByType asserts the canonical harness decodes integers
// strictly by signedness: an unsigned int yields uint64 for both a small value
// and a value above int64; a signed int yields int64; non-integer classes ignore
// signedness.
func TestDriver_DecodeValueByType(t *testing.T) {
	d := New()
	const maxUint64 = "18446744073709551615"

	tests := []struct {
		name string
		typ  core.Type
		raw  string
		want any
	}{
		{"unsigned small", core.Type{Name: TypeInt8, ID: TypeIDInt8, Class: core.TypeClassInt, Unsigned: true}, "42", uint64(42)},
		{"unsigned above int64", core.Type{Name: TypeInt8, ID: TypeIDInt8, Class: core.TypeClassInt, Unsigned: true}, maxUint64, uint64(18446744073709551615)},
		{"signed small", core.Type{Name: TypeInt8, ID: TypeIDInt8, Class: core.TypeClassInt}, "42", int64(42)},
		{"text ignores signedness", core.Type{Name: TypeText, ID: TypeIDText, Class: core.TypeClassText, Unsigned: true}, "abc", "abc"},
		// Name wins over id: a descriptor whose Name is text but whose ID is an
		// integer id decodes as text, never as an unsigned integer. This proves the
		// id never overrides a present Name.
		{"name wins over id", core.Type{Name: TypeText, ID: TypeIDInt8}, "abc", "abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := d.DecodeValueByType(tc.typ, []byte(tc.raw))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	// Resolution by id alone (no name) still works.
	got, err := d.DecodeValueByType(core.Type{ID: TypeIDInt8, Class: core.TypeClassInt, Unsigned: true}, []byte("7"))
	require.NoError(t, err)
	assert.Equal(t, uint64(7), got)
}

// TestCatalogueCoversEveryTypeClass asserts the acceptance-criteria contract:
// every core.TypeClass except TypeClassUnsupported has at least one canonical
// catalogue entry, so a transformer that branches on any class can be exercised.
func TestCatalogueCoversEveryTypeClass(t *testing.T) {
	allClasses := []core.TypeClass{
		core.TypeClassInt,
		core.TypeClassFloat,
		core.TypeClassNumeric,
		core.TypeClassBoolean,
		core.TypeClassText,
		core.TypeClassBinary,
		core.TypeClassDateTime,
		core.TypeClassTime,
		core.TypeClassJson,
		core.TypeClassUuid,
		core.TypeClassOther,
	}

	covered := make(map[core.TypeClass]bool)
	for _, e := range catalogue {
		covered[e.class] = true
	}

	for _, c := range allClasses {
		t.Run(string(c), func(t *testing.T) {
			assert.Truef(t, covered[c], "no catalogue entry for type class %q", c)
		})
	}
}

func TestDriver_RoundTrip(t *testing.T) {
	d := New()

	tests := []struct {
		name     string
		typeID   core.TypeID
		typeName string
		raw      string // canonical wire value
		scanInto func() any
		// want is compared against the dereferenced scan destination.
		want any
	}{
		{"int2", TypeIDInt2, TypeInt2, "-32768", func() any { return new(int64) }, int64(-32768)},
		{"int4", TypeIDInt4, TypeInt4, "2147483647", func() any { return new(int64) }, int64(2147483647)},
		{"int8 max", TypeIDInt8, TypeInt8, "9223372036854775807", func() any { return new(int64) }, int64(9223372036854775807)},
		{"float4", TypeIDFloat4, TypeFloat4, "1.5", func() any { return new(float32) }, float32(1.5)},
		{"float8", TypeIDFloat8, TypeFloat8, "3.141592653589793", func() any { return new(float64) }, float64(3.141592653589793)},
		{"numeric", TypeIDNumeric, TypeNumeric, "12345.6789", func() any { return new(string) }, "12345.6789"},
		{"bool true", TypeIDBool, TypeBool, "1", func() any { return new(bool) }, true},
		{"bool false", TypeIDBool, TypeBool, "0", func() any { return new(bool) }, false},
		{"text", TypeIDText, TypeText, "hello world", func() any { return new(string) }, "hello world"},
		{"text empty", TypeIDText, TypeText, "", func() any { return new(string) }, ""},
		{"bytea", TypeIDBytea, TypeBytea, "\x00\x01\xff", func() any { return new([]byte) }, []byte("\x00\x01\xff")},
		{"json", TypeIDJson, TypeJson, `{"a":1}`, func() any { return new(string) }, `{"a":1}`},
		{"date", TypeIDDate, TypeDate, "2007-09-14", func() any { return new(time.Time) }, time.Date(2007, 9, 14, 0, 0, 0, 0, time.Local)},
		{
			"timestamp", TypeIDTimestamp, TypeTimestamp, "2008-12-15 23:34:17.946707",
			func() any { return new(time.Time) },
			time.Date(2008, 12, 15, 23, 34, 17, 946707000, time.Local),
		},
		{"time", TypeIDTime, TypeTime, "12:30:45", func() any { return new(time.Duration) }, 12*time.Hour + 30*time.Minute + 45*time.Second},
		{"uuid", TypeIDUuid, TypeUuid, "0b13d2c2-76e7-4c8b-8b1a-3f4d5e6a7b8c", func() any { return new(string) }, "0b13d2c2-76e7-4c8b-8b1a-3f4d5e6a7b8c"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// name and id resolve to the same entry.
			assert.True(t, d.TypeExistsByName(tc.typeName))
			assert.True(t, d.TypeExistsByID(tc.typeID))
			gotID, err := d.GetTypeID(tc.typeName)
			require.NoError(t, err)
			assert.Equal(t, tc.typeID, gotID)

			// Scan by name and by the full Type yield the same result.
			dest := tc.scanInto()
			require.NoError(t, d.ScanValueByTypeName(tc.typeName, []byte(tc.raw), dest))
			assert.Equal(t, tc.want, deref(dest))

			typ := core.Type{Name: tc.typeName, ID: tc.typeID}
			dest2 := tc.scanInto()
			require.NoError(t, d.ScanValueByType(typ, []byte(tc.raw), dest2))
			assert.Equal(t, tc.want, deref(dest2))

			// Decode then re-encode by Type reproduces the canonical wire value.
			decoded, err := d.DecodeValueByType(typ, []byte(tc.raw))
			require.NoError(t, err)
			encoded, err := d.EncodeValueByType(typ, decoded, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.raw, string(encoded))
		})
	}
}

func TestDriver_CanonicalTypeClass(t *testing.T) {
	d := New()
	tests := []struct {
		typeName string
		typeID   core.TypeID
		want     core.TypeClass
	}{
		{TypeInt2, TypeIDInt2, core.TypeClassInt},
		{TypeFloat8, TypeIDFloat8, core.TypeClassFloat},
		{TypeNumeric, TypeIDNumeric, core.TypeClassNumeric},
		{TypeBool, TypeIDBool, core.TypeClassBoolean},
		{TypeText, TypeIDText, core.TypeClassText},
		{TypeBytea, TypeIDBytea, core.TypeClassBinary},
		{TypeTimestamp, TypeIDTimestamp, core.TypeClassDateTime},
		{TypeDate, TypeIDDate, core.TypeClassDateTime},
		{TypeTime, TypeIDTime, core.TypeClassTime},
		{TypeJson, TypeIDJson, core.TypeClassJson},
		{TypeUuid, TypeIDUuid, core.TypeClassUuid},
		{TypeOther, TypeIDOther, core.TypeClassOther},
	}
	for _, tc := range tests {
		t.Run(tc.typeName, func(t *testing.T) {
			got, err := d.GetCanonicalTypeClassName(tc.typeName, tc.typeID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			name, err := d.GetCanonicalTypeName(tc.typeName, tc.typeID)
			require.NoError(t, err)
			assert.Equal(t, tc.typeName, name)
		})
	}
}

func TestDriver_UnknownType(t *testing.T) {
	d := New()

	assert.False(t, d.TypeExistsByName("no_such_type"))
	assert.False(t, d.TypeExistsByID(core.TypeID(1)))

	_, err := d.GetTypeID("no_such_type")
	require.Error(t, err)

	_, err = d.GetCanonicalTypeName("no_such_type", core.TypeID(1))
	require.ErrorIs(t, err, core.ErrCanonicalTypeMismatch)

	_, err = d.GetCanonicalTypeClassName("no_such_type", core.TypeID(1))
	require.ErrorIs(t, err, core.ErrUnknownDBMSTypeClass)

	_, err = d.EncodeValueByTypeName("no_such_type", "x", nil)
	require.Error(t, err)
	_, err = d.DecodeValueByType(core.Type{Name: "no_such_type", ID: core.TypeID(1)}, []byte("x"))
	require.Error(t, err)
}

func TestDriver_NullAndEmptyBoundaries(t *testing.T) {
	d := New()

	t.Run("nil scan destination errors", func(t *testing.T) {
		err := d.ScanValueByTypeName(TypeText, []byte("x"), nil)
		require.Error(t, err)
	})

	t.Run("zero time round-trips", func(t *testing.T) {
		encoded, err := d.EncodeValueByTypeName(TypeTimestamp, time.Time{}, nil)
		require.NoError(t, err)
		decoded, err := d.DecodeValueByTypeName(TypeTimestamp, encoded)
		require.NoError(t, err)
		ts, ok := decoded.(time.Time)
		require.True(t, ok)
		assert.True(t, ts.IsZero())
	})

	t.Run("int overflow is reported", func(t *testing.T) {
		var dest int64
		err := d.ScanValueByTypeName(TypeInt8, []byte("99999999999999999999999"), &dest)
		require.Error(t, err)
		assert.ErrorIs(t, err, strconv.ErrRange)
	})

	t.Run("malformed json scans raw bytes", func(t *testing.T) {
		// json codec is a passthrough; malformed content is preserved verbatim
		// so transformers see exactly what the engine emitted.
		var dest string
		require.NoError(t, d.ScanValueByTypeName(TypeJson, []byte("{not json"), &dest))
		assert.Equal(t, "{not json", dest)
	})
}

func deref(p any) any {
	switch v := p.(type) {
	case *int64:
		return *v
	case *uint64:
		return *v
	case *float32:
		return *v
	case *float64:
		return *v
	case *bool:
		return *v
	case *string:
		return *v
	case *[]byte:
		return *v
	case *time.Time:
		return *v
	case *time.Duration:
		return *v
	default:
		return p
	}
}
