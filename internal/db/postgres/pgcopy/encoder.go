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

package pgcopy

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// EncodeAttr - encode from UTF-8 slice to transfer representation (escaped byte[])
func EncodeAttr(v *toolkit.RawValue, buf []byte) []byte {
	// Check whether raw input matched null marker
	if v.IsNull {
		return DefaultNullSeq
	}

	// Escaping every backslash also covers \N and \. inside the data
	for _, c := range v.Data {
		if c < 0x20 {
			// Escaping ASCII control characters
			switch c {
			case '\b':
				c = 'b'
			case '\f':
				c = 'f'
			case '\n':
				c = 'n'
			case '\r':
				c = 'r'
			case '\t':
				c = 't'
			case '\v':
				c = 'v'
			default:
				// Other control characters are written as is, like PostgreSQL does
				buf = append(buf, c)
				continue
			}
			buf = append(buf, '\\', c)
		} else if c == '\\' {
			// Escaping backslash, the delimiter \t is escaped above
			buf = append(buf, '\\', c)
		} else {
			// Add plain rune
			buf = append(buf, c)
		}
	}

	return buf
}
