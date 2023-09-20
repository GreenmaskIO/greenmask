package pgcopy

import (
	"fmt"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecode(t *testing.T) {

	tests := []struct {
		name     string
		original *transformers.RawValue
		expected *transformers.RawValue
	}{
		{
			name:     "simple",
			original: transformers.NewRawValue([]byte("123"), false),
			expected: transformers.NewRawValue([]byte("123"), false),
		}, {
			name:     "back slash escaping",
			original: transformers.NewRawValue([]byte("\\"), false),
			expected: transformers.NewRawValue([]byte("\\"), false),
		},
		{
			name:     "ASCII control chars escaping",
			original: transformers.NewRawValue([]byte("\b\f\n\n\t\v"), false),
			expected: transformers.NewRawValue([]byte("\b\f\n\n\t\v"), false),
		},
		{
			name:     "pgcopy termination symbol",
			original: transformers.NewRawValue([]byte("\\."), false),
			expected: transformers.NewRawValue([]byte("\\."), false),
		},
		{
			name:     "delimiter escaping",
			original: transformers.NewRawValue([]byte(fmt.Sprintf("hello%cnoname", defaultCopyDelimiter)), false),
			expected: transformers.NewRawValue([]byte(fmt.Sprintf("hello%cnoname", defaultCopyDelimiter)), false),
		},
		{
			name:     "Null value",
			original: transformers.NewRawValue(nil, true),
			expected: transformers.NewRawValue(nil, true),
		},
		{
			name:     "Null value in string",
			original: transformers.NewRawValue(defaultNullSeq, false),
			expected: transformers.NewRawValue(defaultNullSeq, false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeAttr(EncodeAttr(tt.original))
			assert.Equal(t, tt.expected.IsNull, res.IsNull, "wrong NULL interpretation found")
			assert.Equal(t, tt.expected.Data, res.Data, "bytes are unequal")
		})
	}
}
