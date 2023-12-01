// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgcopy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestEncodeDecode(t *testing.T) {

	tests := []struct {
		name     string
		original *toolkit.RawValue
		expected *toolkit.RawValue
	}{
		{
			name:     "simple",
			original: toolkit.NewRawValue([]byte("123"), false),
			expected: toolkit.NewRawValue([]byte("123"), false),
		}, {
			name:     "back slash escaping",
			original: toolkit.NewRawValue([]byte("\\"), false),
			expected: toolkit.NewRawValue([]byte("\\"), false),
		},
		{
			name:     "ASCII control chars escaping",
			original: toolkit.NewRawValue([]byte("\b\f\n\n\t\v"), false),
			expected: toolkit.NewRawValue([]byte("\b\f\n\n\t\v"), false),
		},
		{
			name:     "pgcopy termination symbol",
			original: toolkit.NewRawValue([]byte("\\."), false),
			expected: toolkit.NewRawValue([]byte("\\."), false),
		},
		{
			name:     "delimiter escaping",
			original: toolkit.NewRawValue([]byte(fmt.Sprintf("hello%cnoname", DefaultCopyDelimiter)), false),
			expected: toolkit.NewRawValue([]byte(fmt.Sprintf("hello%cnoname", DefaultCopyDelimiter)), false),
		},
		{
			name:     "Null value",
			original: toolkit.NewRawValue(nil, true),
			expected: toolkit.NewRawValue(nil, true),
		},
		{
			name:     "Null value in string",
			original: toolkit.NewRawValue(DefaultNullSeq, false),
			expected: toolkit.NewRawValue(DefaultNullSeq, false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := DecodeAttr(EncodeAttr(tt.original, nil), nil)
			assert.Equal(t, tt.expected.IsNull, res.IsNull, "wrong NULL interpretation found")
			assert.Equal(t, tt.expected.Data, res.Data, "bytes are unequal")
		})
	}
}
