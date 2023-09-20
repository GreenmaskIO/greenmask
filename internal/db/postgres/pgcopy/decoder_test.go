package pgcopy

import (
	"fmt"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
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
		expected *transformers.RawValue
	}{
		{
			name:     "simple",
			original: []byte("123"),
			expected: transformers.NewRawValue([]byte("123"), false),
		},
		{
			name:     "back slash escaping",
			original: []byte("\\\\"),
			expected: transformers.NewRawValue([]byte("\\"), false),
		},
		{
			name:     "ASCII control chars escaping",
			original: []byte("\\b\\f\\n\\n\\t\\v"),
			expected: transformers.NewRawValue([]byte("\b\f\n\n\t\v"), false),
		},
		{
			name:     "pgcopy termination symbol",
			original: []byte("\\\\."),
			expected: transformers.NewRawValue([]byte("\\."), false),
		},
		{
			name:     "delimiter escaping",
			original: []byte("hello\\tnoname"),
			expected: transformers.NewRawValue([]byte(fmt.Sprintf("hello%cnoname", defaultCopyDelimiter)), false),
		},
		{
			name:     "Null value",
			original: []byte("\\N"),
			expected: transformers.NewRawValue(nil, true),
		},
		{
			name:     "Null sequence in text value",
			original: []byte("\\\\N"),
			expected: transformers.NewRawValue([]byte("\\N"), false),
		},
		{
			name:     "Cyrillic",
			original: []byte("здравствуйте"),
			expected: transformers.NewRawValue([]byte("здравствуйте"), false),
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
		expected *transformers.RawValue
	}{
		{
			name:     "Cyrillic octal format",
			original: []byte("\\320\\275\\320\\260"),
			expected: transformers.NewRawValue([]byte("на"), false),
		},
		{
			name:     "Cyrillic hex format",
			original: []byte("\\xD0\\xBd\\xD0\\xB0"),
			expected: transformers.NewRawValue([]byte("на"), false),
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
