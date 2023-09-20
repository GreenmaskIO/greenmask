package pgcopy

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"slices"
)

// EncodeAttr - encode from UTF-8 slice to transfer representation (escaped byte[])
func EncodeAttr(v *transformers.RawValue) []byte {
	// Check whether raw input matched null marker
	if v.IsNull {
		return defaultNullSeq
	}

	data := v.Data
	var res = make([]byte, 0, len(data))

	for i := 0; i < len(data); i++ {
		if len(data[i:]) >= len(defaultNullSeq) && slices.Equal(data[i:i+len(defaultNullSeq)], defaultNullSeq) {
			// Escaping NULL SEQUENCE
			res = append(res, '\\')
			res = append(res, defaultNullSeq...)
			i = i + len(defaultNullSeq)
			continue
		} else if len(data[i:]) >= len(defaultCopyTerminationSeq) && slices.Equal(data[i:i+len(defaultCopyTerminationSeq)], defaultCopyTerminationSeq) {
			// Escaping pgcopy termination string
			res = append(res, '\\')
			res = append(res, defaultCopyTerminationSeq...)
			i = i + len(defaultCopyTerminationSeq)
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
				if c != defaultCopyDelimiter {
					res = append(res, c)
				}
			}
			res = append(res, '\\', c)
		} else if c == '\\' || c == defaultCopyDelimiter {
			// Escaping backslash or pgcopy delimiter
			res = append(res, '\\', c)
		} else {
			// Add plain rune
			res = append(res, c)
		}
	}

	return res
}
