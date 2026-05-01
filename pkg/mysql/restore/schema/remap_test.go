// Copyright 2025 Greenmask
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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRestorer_remapDB(t *testing.T) {
	tests := []struct {
		name  string
		remap map[string]string
		input string
		want  string
	}{
		{
			name:  "mapped name is replaced",
			remap: map[string]string{"src": "dst"},
			input: "src",
			want:  "dst",
		},
		{
			name:  "unmapped name is returned unchanged",
			remap: map[string]string{"src": "dst"},
			input: "other",
			want:  "other",
		},
		{
			name:  "nil map returns input unchanged",
			remap: nil,
			input: "mydb",
			want:  "mydb",
		},
		{
			name:  "empty map returns input unchanged",
			remap: map[string]string{},
			input: "mydb",
			want:  "mydb",
		},
		{
			name:  "multiple entries — correct key selected",
			remap: map[string]string{"a": "x", "b": "y"},
			input: "b",
			want:  "y",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Restorer{databaseRemap: tc.remap}
			assert.Equal(t, tc.want, r.remapDB(tc.input))
		})
	}
}

func TestWithDatabaseRemap(t *testing.T) {
	tests := []struct {
		name      string
		initial   map[string]string
		apply     map[string]string
		wantRemap map[string]string
	}{
		{
			name:      "sets remap on empty restorer",
			initial:   nil,
			apply:     map[string]string{"old": "new"},
			wantRemap: map[string]string{"old": "new"},
		},
		{
			name:      "overwrites previous remap",
			initial:   map[string]string{"a": "b"},
			apply:     map[string]string{"x": "y"},
			wantRemap: map[string]string{"x": "y"},
		},
		{
			name:      "nil remap clears previous mapping",
			initial:   map[string]string{"a": "b"},
			apply:     nil,
			wantRemap: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Restorer{databaseRemap: tc.initial}
			WithDatabaseRemap(tc.apply)(r)
			assert.Equal(t, tc.wantRemap, r.databaseRemap)
		})
	}
}
