package copy

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeAttr(t *testing.T) {
	var a byte = '\n'
	println(a)

	tests := []struct {
		name     string
		original *AttributeValue
		expected []byte
	}{
		{
			name:     "simple",
			original: NewAttributeValue([]byte("123"), false),
			expected: []byte("123"),
		},
		{
			name:     "\\r \\n symbols",
			original: NewAttributeValue([]byte("\r\n"), false),
			expected: []byte("\\r\\n"),
		},
		{
			name:     "Escaped null sequence in text",
			original: NewAttributeValue([]byte("\\N"), false),
			expected: []byte("\\\\N"),
		},
		{
			name:     "Null sequence \\N",
			original: NewAttributeValue(nil, true),
			expected: []byte("\\N"),
		},
		{
			name:     "Escaped end of copy marker \\.",
			original: NewAttributeValue([]byte("\\."), false),
			expected: []byte("\\\\."),
		},
		{
			name:     "Escaped attrs delimiter \\t",
			original: NewAttributeValue([]byte{defaultCopyDelimiter}, false),
			expected: []byte("\\t"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			println(string(tt.expected))
			res := EncodeAttr(tt.original)
			assert.Equal(t, tt.expected, res, "wrong escaped bytes")
		})
	}
}
