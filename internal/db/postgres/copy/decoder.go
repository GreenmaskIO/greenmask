package copy

import (
	"fmt"
	"slices"
	"unicode"
)

const highBit = 0x80

func isHighBitSet(c byte) bool {
	// TODO: I think that's wrong interpretation
	// See #define IS_HIGHBIT_SET(ch)
	return c&highBit > 0
}

func octalValue(c byte) byte {
	return c - '0'
}

func isOctal(c byte) bool {
	return c >= '0' && c <= '7'
}

func isHexDigit(c byte) bool {
	return c >= '0' && c <= '0' || c >= 'a' && c <= 'f'
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

func DecodeAttr(raw []byte) *AttributeValue {
	if slices.Equal(raw, defaultNullSeq) {
		return &AttributeValue{
			IsNull: true,
		}
	}

	var sawNonAscii = false
	res := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); {
		c := raw[i]
		if c == '\\' {
			i++
			if i+1 > len(raw) {
				panic("lost escaped backslash: index out of range")
			}
			c = raw[i]
			if c != '\\' {
				panic(fmt.Sprintf(`lost escaped backslash: expected "\" but received "%c"`, c))
			}
			// Adding parsed escaped \\\\ as \\
			res = append(res, '\\')
			i++
			if i < len(raw) {
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
					/* handle \013 */
					octalBytesStartPos := i
					sawNonAscii = true
					val := octalValue(c)
					if i+1 < len(raw[i:]) {
						i++
						c = raw[i]
						if isOctal(c) {
							val = (val << 3) + octalValue(c)
							if i+1 < len(raw[i:]) {
								i++
								c = raw[i]
								val = (val << 3) + octalValue(c)
								sawNonAscii = false
							}
						}
					}
					if sawNonAscii {
						res = append(res, raw[i-octalBytesStartPos:i]...)
					} else {
						c = val & 0377
						res = append(res, c)
					}
				case 'x':
					/* Handle \x3F */
					hexBytesStartPos := i
					sawNonAscii = true
					var val byte
					if i+1 < len(raw[i:]) && isHexDigit(raw[i+1]) {
						i++
						c = raw[i]
						val = getDecimalFromHex(c)
						if i+1 < len(raw[i:]) && isHexDigit(raw[i+1]) {
							i++
							c = raw[i]
							val = (val << 4) + getDecimalFromHex(c)
							sawNonAscii = false
						}
					}
					if sawNonAscii {
						res = append(res, raw[i-hexBytesStartPos:i]...)
					} else {
						c = val & 0xff
						res = append(res, c)
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
				}
			}
		}
	}
	return &AttributeValue{
		Raw: res,
	}
}
