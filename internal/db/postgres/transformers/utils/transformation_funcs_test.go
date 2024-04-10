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

package utils

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomDateTruncateDate(t *testing.T) {
	loc := time.Now().Location()
	tests := []struct {
		name     string
		part     string
		original time.Time
		expected time.Time
	}{
		{
			name:     "nano",
			part:     "nano",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
		},
		{
			name:     "second",
			part:     "second",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 56, 35, 0, loc),
		},
		{
			name:     "minute",
			part:     "minute",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 56, 0, 0, loc),
		},
		{
			name:     "hour",
			part:     "hour",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 11, 0, 0, 0, loc),
		},
		{
			name:     "day",
			part:     "day",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 10, 0, 0, 0, 0, loc),
		},
		{
			name:     "month",
			part:     "month",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 5, 1, 0, 0, 0, 0, loc),
		},
		{
			name:     "year",
			part:     "year",
			original: time.Date(2023, 5, 10, 11, 56, 35, 123456, loc),
			expected: time.Date(2023, 1, 1, 0, 0, 0, 0, loc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := TruncateDate(&tt.part, &tt.original)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, *res)
		})
	}
}

func TestRandomString(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixMicro()))
	tests := []struct {
		name    string
		min     int64
		max     int64
		symbols []rune
	}{
		{
			name:    "min length",
			min:     10,
			max:     20,
			symbols: []rune("0123456789"),
		},
		{
			name:    "max length",
			min:     10,
			max:     20,
			symbols: []rune("abcdefg12345678990,al-"),
		},
		{
			name:    "empty symbols",
			min:     20,
			max:     20,
			symbols: []rune("0123456789"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]rune, tt.max)
			res := RandomString(r, tt.min, tt.max, tt.symbols, buf)
			require.True(t, len(res) >= int(tt.min))
			for idx, ch := range res {
				assert.Containsf(t, tt.symbols, ch, "unexpected symbol [%d]=%c", idx, ch)
			}
		})
	}
}
