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

package restorers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func TestNewAclRestorer(t *testing.T) {
	entry := &toc.Entry{
		DumpId: 123,
		Tag:    strPtr("TABLE users"),
		Desc:   strPtr(toc.AclDesc),
		Defn:   strPtr("GRANT SELECT ON TABLE public.users TO readonly;"),
	}

	restorer := NewAclRestorer(entry)

	require.NotNil(t, restorer)
	assert.Equal(t, entry, restorer.Entry)
}

func TestAclRestorer_GetEntry(t *testing.T) {
	entry := &toc.Entry{
		DumpId: 123,
		Tag:    strPtr("TABLE users"),
		Desc:   strPtr(toc.AclDesc),
		Defn:   strPtr("GRANT SELECT ON TABLE public.users TO readonly;"),
	}

	restorer := NewAclRestorer(entry)
	assert.Equal(t, entry, restorer.GetEntry())
}

func TestAclRestorer_DebugInfo(t *testing.T) {
	tests := []struct {
		name     string
		entry    *toc.Entry
		expected string
	}{
		{
			name: "with tag",
			entry: &toc.Entry{
				Tag: strPtr("TABLE users"),
			},
			expected: "ACL TABLE users",
		},
		{
			name:     "without tag",
			entry:    &toc.Entry{},
			expected: "ACL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restorer := NewAclRestorer(tt.entry)
			assert.Equal(t, tt.expected, restorer.DebugInfo())
		})
	}
}

// Helper function
func strPtr(s string) *string {
	return &s
}
