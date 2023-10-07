package pgcopy

import (
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/assert"
)

func TestEncodeAttr(t *testing.T) {
	var a byte = '\n'
	println(a)

	tests := []struct {
		name     string
		original *toolkit.RawValue
		expected []byte
	}{
		{
			name:     "simple",
			original: toolkit.NewRawValue([]byte("123"), false),
			expected: []byte("123"),
		},
		{
			name:     "\\r \\n symbols",
			original: toolkit.NewRawValue([]byte("\r\n"), false),
			expected: []byte("\\r\\n"),
		},
		{
			name:     "Escaped null sequence in text",
			original: toolkit.NewRawValue([]byte("\\N"), false),
			expected: []byte("\\\\N"),
		},
		{
			name:     "Null sequence \\N",
			original: toolkit.NewRawValue(nil, true),
			expected: []byte("\\N"),
		},
		{
			name:     "Escaped end of pgcopy marker \\.",
			original: toolkit.NewRawValue([]byte("\\."), false),
			expected: []byte("\\\\."),
		},
		{
			name:     "Escaped attrs delimiter \\t",
			original: toolkit.NewRawValue([]byte{DefaultCopyDelimiter}, false),
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
