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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
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
			res := EncodeAttr(tt.original, nil)
			assert.Equal(t, tt.expected, res, "wrong escaped bytes")
		})
	}
}
