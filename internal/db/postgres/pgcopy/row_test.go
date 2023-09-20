package pgcopy

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewRow(t *testing.T) {

	type result struct {
		pos    []*columnPos
		values [][]byte
	}

	tests := []struct {
		name     string
		original []byte
		result   result
	}{
		{
			name:     "multi row",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N"),
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
			row := NewRow(tt.original)
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
		result   *transformers.RawValue
		idx      int
	}{
		{
			name:     "simple column",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   transformers.NewRawValue([]byte("27"), false),
			idx:      0,
		},
		{
			name:     "column with escaped symbols",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   transformers.NewRawValue([]byte("hey\tmyname is\nnoname"), false),
			idx:      1,
		},
		{
			name:     "null value",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   transformers.NewRawValue(nil, true),
			idx:      2,
		},
		{
			name:     "last null value in line",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			result:   transformers.NewRawValue(nil, true),
			idx:      3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := NewRow(tt.original)
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
		value *transformers.RawValue
	}

	tests := []struct {
		name     string
		original []byte
		params   params
		expected []byte
	}{
		{
			name:     "set literal",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			params: params{
				idx:   1,
				value: transformers.NewRawValue([]byte("\tnew_value\n"), false),
			},
			expected: []byte("27\t\\tnew_value\\n\t\\N\t\\N"),
		},
		{
			name:     "set null value",
			original: []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			params: params{
				idx:   0,
				value: transformers.NewRawValue(nil, true),
			},
			expected: []byte("\\N\they\\tmyname is\\nnoname\t\\N\t\\N"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := NewRow(tt.original)
			err := row.SetColumn(tt.params.idx, tt.params.value)
			require.NoError(t, err)
			res, err := row.Encode()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, res)
		})
	}
}

func TestRow_Decode(t *testing.T) {

	tests := []struct {
		name      string
		original  []byte
		expected  []*transformers.RawValue
		newVal    *transformers.RawValue
		newValIdx int
	}{
		{
			name:      "common",
			original:  []byte("27\they\\tmyname is\\nnoname\t\\N\t\\N"),
			newVal:    transformers.NewRawValue([]byte("1\n2"), false),
			newValIdx: 3,
			expected: []*transformers.RawValue{
				transformers.NewRawValue([]byte("27"), false),
				transformers.NewRawValue([]byte("hey\tmyname is\nnoname"), false),
				transformers.NewRawValue(nil, true),
				transformers.NewRawValue([]byte("1\n2"), false),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := NewRow(tt.original)
			err := row.SetColumn(tt.newValIdx, tt.newVal)
			require.NoError(t, err)
			res, err := row.Decode()
			require.NoError(t, err)
			for idx := range res {
				assert.Equal(t, tt.expected[idx].IsNull, res[idx].IsNull)
				assert.Equal(t, tt.expected[idx].Data, res[idx].Data)
			}

		})
	}
}
