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

package dbmsdriver_test

import (
	"fmt"
	"testing"

	"github.com/huandu/go-sqlbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
)

// oracle reproduces exactly how InsertWriter.Write previously produced the quoted
// literal: Interpolate("?", []interface{}{string(val)}) on the MySQL flavor.
func oracle(t testing.TB, val []byte) string {
	t.Helper()
	s, err := sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
	require.NoError(t, err)
	return s
}

func TestAppendMySQLQuotedString(t *testing.T) {
	// All 256 single-byte values, each in isolation.
	allBytes := make([]struct {
		name string
		val  []byte
	}, 0, 256)
	for b := 0; b < 256; b++ {
		allBytes = append(allBytes, struct {
			name string
			val  []byte
		}{fmt.Sprintf("byte_0x%02x", b), []byte{byte(b)}})
	}

	tests := []struct {
		name string
		val  []byte
	}{
		{"empty", []byte{}},
		{"nil", nil},
		{"plain_ascii", []byte("hello world")},
		{"escape_nul", []byte{0x00}},
		{"escape_backspace", []byte{'\b'}},
		{"escape_tab", []byte{'\t'}},
		{"escape_newline", []byte{'\n'}},
		{"escape_carriage_return", []byte{'\r'}},
		{"escape_sub", []byte{0x1a}},
		{"escape_single_quote", []byte{'\''}},
		{"escape_double_quote", []byte{'"'}},
		{"escape_backslash", []byte{'\\'}},
		{"all_escapes_adjacent", []byte{0x00, '\b', '\t', '\n', '\r', 0x1a, '\'', '"', '\\'}},
		{"escapes_with_ascii", []byte("a\tb\nc'd\"e\\f")},
		{"multibyte_utf8", []byte("héllo→你好")},
		{"invalid_utf8", []byte{0xff, 0xfe, 0x80}},
		{"null_marker", []byte(`\N`)},
		{"escaped_null_marker", []byte(`\\N`)},
		{"long_mixed", []byte("The quick brown fox\tjumps over\nthe 'lazy' \"dog\" \\ 你好 \x00 end")},
	}
	tests = append(tests, allBytes...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := string(dbmsdriver.AppendMySQLQuotedString(nil, tc.val))
			want := oracle(t, tc.val)
			assert.Equal(t, want, got)
		})
	}
}

// TestAppendMySQLQuotedString_AppendsToBuffer verifies the function appends to
// (rather than overwrites) the supplied buffer and is reusable via slicing to
// length zero.
func TestAppendMySQLQuotedString_AppendsToBuffer(t *testing.T) {
	buf := []byte("prefix:")
	buf = dbmsdriver.AppendMySQLQuotedString(buf, []byte("a'b"))
	assert.Equal(t, "prefix:"+oracle(t, []byte("a'b")), string(buf))

	// Reuse the same backing array via buf[:0].
	reuse := dbmsdriver.AppendMySQLQuotedString(buf[:0], []byte("xyz"))
	assert.Equal(t, oracle(t, []byte("xyz")), string(reuse))
}

// TestAppendMySQLQuotedString_AllTwoBytePairs exhaustively checks every 2-byte
// input (0x0000..0xffff) against the oracle. This is the strongest deterministic
// guard against lead-byte/continuation-byte interactions — e.g. a special ASCII
// byte (< 0x80) following a UTF-8 lead byte (>= 0xc0). If the rune decoder ever
// "swallowed" a special byte into a multi-byte sequence, this would catch it.
func TestAppendMySQLQuotedString_AllTwoBytePairs(t *testing.T) {
	val := make([]byte, 2)
	for hi := 0; hi < 256; hi++ {
		for lo := 0; lo < 256; lo++ {
			val[0] = byte(hi)
			val[1] = byte(lo)
			got := string(dbmsdriver.AppendMySQLQuotedString(nil, val))
			want, err := sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
			require.NoErrorf(t, err, "interpolate %#v", val)
			if got != want {
				t.Fatalf("mismatch for bytes [0x%02x 0x%02x]: got %q want %q", hi, lo, got, want)
			}
		}
	}
}

// TestAppendMySQLQuotedString_SpecialBytesAmidUTF8 places each of the nine
// escaped bytes between every UTF-8 lead byte and assorted continuation bytes,
// across 3- and 4-byte sequences, to confirm a special byte adjacent to truncated
// or malformed multi-byte runs is still escaped identically to the oracle.
func TestAppendMySQLQuotedString_SpecialBytesAmidUTF8(t *testing.T) {
	special := []byte{0x00, '\b', '\t', '\n', '\r', 0x1a, '\'', '"', '\\'}
	conts := []byte{0x80, 0xbf, 0xc0, 0x41, 0xf4} // valid continuations + an ASCII + an out-of-range
	for lead := 0xc0; lead <= 0xff; lead++ {
		for _, sp := range special {
			for _, c := range conts {
				seqs := [][]byte{
					{byte(lead), sp},
					{byte(lead), sp, c},
					{byte(lead), c, sp},
					{byte(lead), c, sp, c},
					{sp, byte(lead), c},
				}
				for _, val := range seqs {
					got := string(dbmsdriver.AppendMySQLQuotedString(nil, val))
					want, err := sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
					require.NoErrorf(t, err, "interpolate %#v", val)
					if got != want {
						t.Fatalf("mismatch for %#v: got %q want %q", val, got, want)
					}
				}
			}
		}
	}
}

func FuzzMySQLQuote(f *testing.F) {
	seeds := [][]byte{
		{},
		[]byte("hello world"),
		{0x00, '\b', '\t', '\n', '\r', 0x1a, '\'', '"', '\\'},
		[]byte("héllo→你好"),
		{0xff, 0xfe, 0x80},
		[]byte(`\N`),
		[]byte(`\\N`),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, val []byte) {
		got := string(dbmsdriver.AppendMySQLQuotedString(nil, val))
		want, err := sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}

// benchValues is a representative mix: short ASCII, text needing escapes, and a
// longer string.
var benchValues = [][]byte{
	[]byte("12345"),
	[]byte("a short string"),
	[]byte("o'brien said \"hi\"\tand left\n"),
	[]byte("The quick brown fox jumps over the lazy dog, repeatedly, for a while."),
}

func BenchmarkInterpolateOld(b *testing.B) {
	//BenchmarkInterpolateOld-10    	 1937839	       601.4 ns/op	     485 B/op	      15 allocs/op
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, val := range benchValues {
			s, err := sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
			if err != nil {
				b.Fatal(err)
			}
			_ = s
		}
	}
}

func BenchmarkAppendMySQLQuotedNew(b *testing.B) {
	b.ReportAllocs()
	var buf []byte
	for i := 0; i < b.N; i++ {
		for _, val := range benchValues {
			buf = dbmsdriver.AppendMySQLQuotedString(buf[:0], val)
		}
	}
}
