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
	"slices"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// EncodeAttr - encode from UTF-8 slice to transfer representation (escaped byte[])
func EncodeAttr(v *toolkit.RawValue, buf []byte) []byte {
	// Check whether raw input matched null marker
	if v.IsNull {
		return DefaultNullSeq
	}

	data := v.Data

	for i := 0; i < len(data); i++ {
		if len(data[i:]) >= len(DefaultNullSeq) && slices.Equal(data[i:i+len(DefaultNullSeq)], DefaultNullSeq) {
			// Escaping NULL SEQUENCE
			buf = append(buf, '\\')
			buf = append(buf, DefaultNullSeq...)
			i = i + len(DefaultNullSeq)
			continue
		} else if len(data[i:]) >= len(DefaultCopyTerminationSeq) && slices.Equal(data[i:i+len(DefaultCopyTerminationSeq)], DefaultCopyTerminationSeq) {
			// Escaping pgcopy termination string
			buf = append(buf, '\\')
			buf = append(buf, DefaultCopyTerminationSeq...)
			i = i + len(DefaultCopyTerminationSeq)
			continue
		}

		c := data[i]
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
				// TODO: Recheck it
				// As I understand if current ASCII control symb is not equal as the listed we are writing it directly
				if c != DefaultCopyDelimiter {
					buf = append(buf, c)
				}
			}
			buf = append(buf, '\\', c)
		} else if c == '\\' || c == DefaultCopyDelimiter {
			// Escaping backslash or pgcopy delimiter
			buf = append(buf, '\\', c)
		} else {
			// Add plain rune
			buf = append(buf, c)
		}
	}

	return buf
}
