package copy

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecodeAttr(t *testing.T) {
	var a byte = '\n'
	println(a)

	tests := []struct {
		name     string
		original []byte
		expected *AttributeValue
	}{
		{
			name:     "simple",
			original: []byte("123"),
			expected: NewAttributeValue([]byte("123"), false),
		},
		{
			name:     "back slash escaping",
			original: []byte("\\\\"),
			expected: NewAttributeValue([]byte("\\"), false),
		},
		{
			name:     "ASCII control chars escaping",
			original: []byte("\\b\\f\\n\\n\\t\\v"),
			expected: NewAttributeValue([]byte("\b\f\n\n\t\v"), false),
		},
		{
			name:     "copy termination symbol",
			original: []byte("\\\\."),
			expected: NewAttributeValue([]byte("\\."), false),
		},
		{
			name:     "delimiter escaping",
			original: []byte("hello\\tnoname"),
			expected: NewAttributeValue([]byte(fmt.Sprintf("hello%cnoname", defaultCopyDelimiter)), false),
		},
		{
			name:     "Null value",
			original: []byte("\\N"),
			expected: NewAttributeValue(nil, true),
		},
		{
			name:     "Null sequence in text value",
			original: []byte("\\\\N"),
			expected: NewAttributeValue([]byte("\\N"), false),
		},
		{
			name:     "Cyrillic",
			original: []byte("здравствуйте"),
			expected: NewAttributeValue([]byte("здравствуйте"), false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeAttr(tt.original)
			assert.Equal(t, tt.expected.IsNull, res.IsNull)
			if !res.IsNull {
				assert.Equal(t, tt.expected.Data, res.Data, "wrong escaped bytes")
			}
		})
	}
}

func TestDecodeAttr_backslash_panic(t *testing.T) {
	require.PanicsWithValue(t, "backslash cannot be alone", func() {
		DecodeAttr([]byte("\\"))
	})
}

func TestDecodeAttr_non_acii_symbols(t *testing.T) {
	tests := []struct {
		name     string
		original []byte
		expected *AttributeValue
	}{
		{
			name:     "Cyrillic octal format",
			original: []byte("\\320\\275\\320\\260"),
			expected: NewAttributeValue([]byte("на"), false),
		},
		{
			name:     "Cyrillic hex format",
			original: []byte("\\xD0\\xBd\\xD0\\xB0"),
			expected: NewAttributeValue([]byte("на"), false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeAttr(tt.original)
			assert.Equal(t, tt.expected.IsNull, res.IsNull)
			if !res.IsNull {
				assert.Equal(t, tt.expected.Data, res.Data, "wrong escaped bytes")
			}
		})
	}
}
