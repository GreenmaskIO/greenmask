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
	"unicode"
	"unicode/utf8"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const highBit byte = 0x80

func isHighBitSet(c byte) bool {
	return c&highBit > 0
}

func octalValue(c byte) byte {
	return c - '0'
}

func isOctal(c byte) bool {
	return c >= '0' && c <= '7'
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func getDecimalFromHex(c byte) byte {
	if isDigit(c) {
		return c - '0'
	}
	return byte(unicode.ToLower(rune(c))) - 'a' + 10
}

func DecodeAttr(raw []byte, buf []byte) *toolkit.RawValue {
	if slices.Equal(raw, DefaultNullSeq) {
		return &toolkit.RawValue{
			IsNull: true,
		}
	}

	var sawNonAscii = false
	for i := 0; i < len(raw); {
		c := raw[i]
		if c == '\\' {
			if i+1 >= len(raw) {
				// It's not expected that backslash is alone
				panic("backslash cannot be alone")
			}
			i++
			c = raw[i]

			switch c {
			case '0':
				fallthrough
			case '1':
				fallthrough
			case '2':
				fallthrough
			case '3':
				fallthrough
			case '4':
				fallthrough
			case '5':
				fallthrough
			case '6':
				fallthrough
			case '7':
				// Translating textual ASCII symbols written in octal format
				/* handle \013 */
				val := octalValue(c)
				if i+1 < len(raw) && isOctal(raw[i+1]) {
					i++
					c = raw[i]
					val = (val << 3) + octalValue(c)
					if i+1 < len(raw) && isOctal(raw[i+1]) {
						i++
						c = raw[i]
						val = (val << 3) + octalValue(c)
					}
				}
				c = val & 0377
				if c == 0 || isHighBitSet(c) {
					sawNonAscii = true
				}

			case 'x':
				// Translating textual ASCII symbols written in hex format
				/* Handle \x3F */
				var val byte
				if i+1 < len(raw) && isHexDigit(raw[i+1]) {
					i++
					c = raw[i]
					val = getDecimalFromHex(c)
					if i+1 < len(raw) && isHexDigit(raw[i+1]) {
						i++
						c = raw[i]
						val = (val << 4) + getDecimalFromHex(c)
					}
				}
				c = val & 0xff
				if c == 0 && isHighBitSet(c) {
					sawNonAscii = true
				}
			case 'b':
				c = '\b'
			case 'f':
				c = '\f'
			case 'n':
				c = '\n'
			case 'r':
				c = '\r'
			case 't':
				c = '\t'
			case 'v':
				c = '\v'

				/*
				 * default: in all other cases, take the char after '\'
				 * literally
				 */
			}
		}
		buf = append(buf, c)
		i++
	}
	if sawNonAscii && !utf8.Valid(buf) {
		panic("error checking UTF-8 string after non ASCII symbols decoding")
	}

	return &toolkit.RawValue{
		Data: buf,
	}
}
