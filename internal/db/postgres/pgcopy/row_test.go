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
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestDecode(t *testing.T) {

	type result struct {
		pos    []*columnPos
		values [][]byte
	}

	tests := []struct {
		name     string
		original []byte
		result   result
		length   int
	}{
		{
			name:     "multi row",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N"),
			length:   3,
			result: result{
				pos: []*columnPos{
					{
						start: 0,
						end:   2,
					},
					{
						start: 3,
						end:   25,
					},
					{
						start: 26,
						end:   28,
					},
				},
				values: [][]byte{
					[]byte("27"),
					[]byte("hey\\tmyname is\\nnoname"),
					[]byte("\\N"),
				},
			},
		}, {
			name:     "one row",
			original: []byte("27"),
			length:   1,
			result: result{
				pos: []*columnPos{
					{
						start: 0,
						end:   2,
					},
				},
				values: [][]byte{
					[]byte("27"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := NewRow(tt.length)
			err := row.Decode(tt.original)
			require.NoError(t, err)
			for idx := range row.columnPos {
				assert.Equalf(t, tt.result.pos[idx].start, row.columnPos[idx].start, "column %d: start position are unequal", idx)
				assert.Equalf(t, tt.result.pos[idx].end, row.columnPos[idx].end, "column %d: end position are unequal", idx)
			}
			for idx := range row.columnPos {
				start := row.columnPos[idx].start
				end := row.columnPos[idx].end
				assert.Equalf(t, tt.result.values[idx], row.raw[start:end], "column %d: unexpected value", idx)
			}
		})
	}
}

func TestRow_GetColumn(t *testing.T) {

	tests := []struct {
		name     string
		original []byte
		result   *toolkit.RawValue
		idx      int
		length   int
	}{
		{
			name:     "simple column",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   toolkit.NewRawValue([]byte("27"), false),
			idx:      0,
			length:   4,
		},
		{
			name:     "column with escaped symbols",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   toolkit.NewRawValue([]byte("hey\tmyname is\nnoname"), false),
			idx:      1,
			length:   4,
		},
		{
			name:     "null value",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   toolkit.NewRawValue(nil, true),
			idx:      2,
			length:   4,
		},
		{
			name:     "last null value in line",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   toolkit.NewRawValue(nil, true),
			idx:      3,
			length:   4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := NewRow(tt.length)
			err := row.Decode(tt.original)
			require.NoError(t, err)
			av, err := row.GetColumn(tt.idx)
			require.NoError(t, err)
			assert.Equal(t, av.IsNull, tt.result.IsNull)
			assert.Equal(t, av.Data, tt.result.Data)
		})
	}
}

func TestRow_SetColumn_Encoding(t *testing.T) {

	type params struct {
		idx   int
		value *toolkit.RawValue
	}

	tests := []struct {
		name     string
		original []byte
		params   params
		expected []byte
		length   int
	}{
		{
			name:     "set literal",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			params: params{
				idx:   1,
				value: toolkit.NewRawValue([]byte("\tnew_value\n"), false),
			},
			expected: []byte("27\t\\tnew_value\\n\t\\N\t\\N"),
			length:   4,
		},
		{
			name:     "set null value",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			params: params{
				idx:   0,
				value: toolkit.NewRawValue(nil, true),
			},
			expected: []byte("\\N\they\\tmyname is\\nnoname\t\\N\t\\N"),
			length:   4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := NewRow(tt.length)
			err := row.Decode(tt.original)
			require.NoError(t, err)
			err = row.SetColumn(tt.params.idx, tt.params.value)
			require.NoError(t, err)
			res, err := row.Encode()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, res)
		})
	}
}
