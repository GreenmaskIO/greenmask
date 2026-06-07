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

package dump

import (
	"testing"

	"github.com/stretchr/testify/assert"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func TestParseServerVersion(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		comment    string
		wantVendor string
		wantMajor  int
		wantMinor  int
		wantPatch  int
	}{
		{
			name:       "mysql plain",
			version:    "8.0.35",
			comment:    "MySQL Community Server - GPL",
			wantVendor: core.DBMSVendorMySQL,
			wantMajor:  8, wantMinor: 0, wantPatch: 35,
		},
		{
			name:       "mysql with distro suffix",
			version:    "8.0.35-0ubuntu0.20.04.1",
			comment:    "(Ubuntu)",
			wantVendor: core.DBMSVendorMySQL,
			wantMajor:  8, wantMinor: 0, wantPatch: 35,
		},
		{
			name:       "mysql 5.7",
			version:    "5.7.42",
			comment:    "MySQL Community Server (GPL)",
			wantVendor: core.DBMSVendorMySQL,
			wantMajor:  5, wantMinor: 7, wantPatch: 42,
		},
		{
			name:       "mariadb suffix",
			version:    "10.11.5-MariaDB",
			comment:    "mariadb.org binary distribution",
			wantVendor: core.DBMSVendorMariaDB,
			wantMajor:  10, wantMinor: 11, wantPatch: 5,
		},
		{
			name:       "mariadb with compat prefix",
			version:    "5.5.5-10.11.5-MariaDB-1:10.11.5+maria~ubu2204",
			comment:    "mariadb.org binary distribution",
			wantVendor: core.DBMSVendorMariaDB,
			wantMajor:  10, wantMinor: 11, wantPatch: 5,
		},
		{
			name:       "mariadb detected via comment only",
			version:    "10.6.16",
			comment:    "Source distribution mariadb",
			wantVendor: core.DBMSVendorMariaDB,
			wantMajor:  10, wantMinor: 6, wantPatch: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseServerVersion(tt.version, tt.comment)
			assert.Equal(t, tt.version, got.FullString)
			assert.Equal(t, tt.wantMajor, got.Major)
			assert.Equal(t, tt.wantMinor, got.Minor)
			assert.Equal(t, tt.wantPatch, got.Patch)
			assert.Equal(t, tt.wantVendor, got.Vendor())
			assert.Equal(t, tt.wantVendor, got.Metadata[core.DBMSVendorKey])
			if tt.comment != "" {
				assert.Equal(t, tt.comment, got.Metadata[core.DBMSVersionCommentKey])
			}
		})
	}
}

func TestParseServerVersion_malformed(t *testing.T) {
	// Missing/garbage components must not panic and default to zero.
	got := parseServerVersion("", "")
	assert.Equal(t, core.DBMSVendorMySQL, got.Vendor())
	assert.Equal(t, 0, got.Major)

	got = parseServerVersion("8", "")
	assert.Equal(t, 8, got.Major)
	assert.Equal(t, 0, got.Minor)
	assert.Equal(t, 0, got.Patch)
}
