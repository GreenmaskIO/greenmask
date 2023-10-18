package pgcopy

import (
	"fmt"
	"testing"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeAttr(t *testing.T) {
	var a byte = '\n'
	println(a)

	tests := []struct {
		name     string
		original []byte
		expected *toolkit.RawValue
	}{
		{
			name:     "simple",
			original: []byte("123"),
			expected: toolkit.NewRawValue([]byte("123"), false),
		},
		{
			name:     "back slash escaping",
			original: []byte("\\\\"),
			expected: toolkit.NewRawValue([]byte("\\"), false),
		},
		{
			name:     "ASCII control chars escaping",
			original: []byte("\\b\\f\\n\\n\\t\\v"),
			expected: toolkit.NewRawValue([]byte("\b\f\n\n\t\v"), false),
		},
		{
			name:     "pgcopy termination symbol",
			original: []byte("\\\\."),
			expected: toolkit.NewRawValue([]byte("\\."), false),
		},
		{
			name:     "delimiter escaping",
			original: []byte("hello\\tnoname"),
			expected: toolkit.NewRawValue([]byte(fmt.Sprintf("hello%cnoname", DefaultCopyDelimiter)), false),
		},
		{
			name:     "Null value",
			original: []byte("\\N"),
			expected: toolkit.NewRawValue(nil, true),
		},
		{
			name:     "Null sequence in text value",
			original: []byte("\\\\N"),
			expected: toolkit.NewRawValue([]byte("\\N"), false),
		},
		{
			name:     "Cyrillic",
			original: []byte("здравствуйте"),
			expected: toolkit.NewRawValue([]byte("здравствуйте"), false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeAttr(tt.original, nil)
			assert.Equal(t, tt.expected.IsNull, res.IsNull)
			if !res.IsNull {
				assert.Equal(t, tt.expected.Data, res.Data, "wrong escaped bytes")
			}
		})
	}
}

func TestDecodeAttr_backslash_panic(t *testing.T) {
	require.PanicsWithValue(t, "backslash cannot be alone", func() {
		DecodeAttr([]byte("\\"), nil)
	})
}

func TestDecodeAttr_non_acii_symbols(t *testing.T) {
	tests := []struct {
		name     string
		original []byte
		expected *toolkit.RawValue
	}{
		{
			name:     "Cyrillic octal format",
			original: []byte("\\320\\275\\320\\260"),
			expected: toolkit.NewRawValue([]byte("на"), false),
		},
		{
			name:     "Cyrillic hex format",
			original: []byte("\\xD0\\xBd\\xD0\\xB0"),
			expected: toolkit.NewRawValue([]byte("на"), false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeAttr(tt.original, nil)
			assert.Equal(t, tt.expected.IsNull, res.IsNull)
			if !res.IsNull {
				assert.Equal(t, tt.expected.Data, res.Data, "wrong escaped bytes")
			}
		})
	}
}
