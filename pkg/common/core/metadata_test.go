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

package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSchemaDumpMetadata_VendorUtility(t *testing.T) {
	mysqldump := &VendorUtility{Name: "mysqldump", VersionString: "8.0.35", VersionParts: []string{"8", "0", "35"}}

	tests := []struct {
		name       string
		stats      []SchemaDumpStat
		wantNil    bool
		wantVendor *VendorUtility
		wantOrigSz int64
		wantCompSz int64
	}{
		{
			name:    "no stats returns nil metadata",
			stats:   nil,
			wantNil: true,
		},
		{
			name: "promotes vendor utility from first stat that carries it",
			stats: []SchemaDumpStat{
				{Section: DumpSectionPreData, OriginalSize: 10, CompressedSize: 4},
				{Section: DumpSectionPostData, OriginalSize: 20, CompressedSize: 6, VendorUtility: mysqldump},
			},
			wantVendor: mysqldump,
			wantOrigSz: 30,
			wantCompSz: 10,
		},
		{
			name: "no vendor utility on any stat leaves it nil",
			stats: []SchemaDumpStat{
				{Section: DumpSectionPreData, OriginalSize: 1, CompressedSize: 1},
			},
			wantVendor: nil,
			wantOrigSz: 1,
			wantCompSz: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := NewSchemaDumpMetadata(tc.stats)
			if tc.wantNil {
				assert.Nil(t, md)
				return
			}
			require.NotNil(t, md)
			assert.Equal(t, tc.wantVendor, md.VendorUtility)
			assert.Equal(t, tc.wantOrigSz, md.OriginalSize)
			assert.Equal(t, tc.wantCompSz, md.CompressedSize)
		})
	}
}
