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

// AppendMySQLQuotedString appends val to buf as a single-quoted MySQL string literal.
//
// It is byte-for-byte equivalent to
//
//	sqlbuilder.MySQL.Interpolate("?", []interface{}{string(val)})
//
// for string arguments. go-sqlbuilder's quoteStringValue walks the value rune by
// rune, but every byte it escapes is ASCII (< 0x80) and an ASCII byte can never
// appear inside a multi-byte UTF-8 sequence (lead and continuation bytes are all
// >= 0x80). Invalid UTF-8 is emitted byte by byte unchanged. Therefore this plain
// byte-by-byte loop produces identical output for valid UTF-8, invalid UTF-8, and
// raw binary alike, with no UTF-8 decoding required.
//
// Only the MySQL string branch is replicated here: the caller already stringifies
// every column, so the numeric/time/bool/Valuer branches and the _binary prefix
// (emitted only for raw []byte args, which are never passed) are out of scope.
func AppendMySQLQuotedString(buf, val []byte) []byte {
	buf = append(buf, '\'')
	for _, b := range val {
		switch b {
		case 0x00:
			buf = append(buf, '\\', '0')
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\t':
			buf = append(buf, '\\', 't')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case 0x1a:
			buf = append(buf, '\\', 'Z')
		case '\'':
			buf = append(buf, '\\', '\'')
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		default:
			buf = append(buf, b)
		}
	}
	buf = append(buf, '\'')
	return buf
}
