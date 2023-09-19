package copy

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecode(t *testing.T) {

	tests := []struct {
		name     string
		original *AttributeValue
		expected *AttributeValue
	}{
		{
			name:     "simple",
			original: NewAttributeValue([]byte("123"), false),
			expected: NewAttributeValue([]byte("123"), false),
		}, {
			name:     "back slash escaping",
			original: NewAttributeValue([]byte("\\"), false),
			expected: NewAttributeValue([]byte("\\"), false),
		},
		{
			name:     "ASCII control chars escaping",
			original: NewAttributeValue([]byte("\b\f\n\n\t\v"), false),
			expected: NewAttributeValue([]byte("\b\f\n\n\t\v"), false),
		},
		{
			name:     "copy termination symbol",
			original: NewAttributeValue([]byte("\\."), false),
			expected: NewAttributeValue([]byte("\\."), false),
		},
		{
			name:     "delimiter escaping",
			original: NewAttributeValue([]byte(fmt.Sprintf("hello%cnoname", defaultCopyDelimiter)), false),
			expected: NewAttributeValue([]byte(fmt.Sprintf("hello%cnoname", defaultCopyDelimiter)), false),
		},
		{
			name:     "Null value",
			original: NewAttributeValue(nil, true),
			expected: NewAttributeValue(nil, true),
		},
		{
			name:     "Null value in string",
			original: NewAttributeValue(defaultNullSeq, false),
			expected: NewAttributeValue(defaultNullSeq, false),
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
