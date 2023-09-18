package copy

import "slices"

type AttributeValue struct {
	Raw    []byte
	IsNull bool
}

func NewAttributeValue(raw []byte, isNull bool) *AttributeValue {
	return &AttributeValue{
		Raw:    raw,
		IsNull: isNull,
	}
}

// EncodeAttr - encode from string in slice to transfer representation (escaped byte[])
func EncodeAttr(v *AttributeValue) []byte {
	// Check whether raw input matched null marker
	if v.IsNull {
		return defaultNullSeq
	}

	data := v.Raw
	var res = make([]byte, 0, len(data))

	for i := 0; i < len(data); i++ {
		if len(data[i:]) >= len(defaultNullSeq) && slices.Equal(data[i:i+len(defaultNullSeq)], defaultNullSeq) {
			// Escaping NULL SEQUENCE
			res = append(res, '\\')
			res = append(res, defaultNullSeq...)
			i = i + len(defaultNullSeq)
			continue
		} else if len(data[i:]) >= len(defaultCopyTerminationSeq) && slices.Equal(data[i:i+len(defaultCopyTerminationSeq)], defaultCopyTerminationSeq) {
			// Escaping copy termination string
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
			res = append(res, '\\', '\\', c)
		} else if c == '\\' || c == defaultCopyDelimiter {
			// Escaping backslash or copy delimiter
			res = append(res, '\\', '\\', c)
		} else {
			// Add plain rune
			res = append(res, c)
		}
	}

	return res
}
