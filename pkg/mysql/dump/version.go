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
	"strconv"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// mariadbCompatPrefix is the "replication version" prefix MariaDB (>= 10) prepends
// to VERSION() over the legacy protocol so old MySQL clients tolerate the major
// version. We strip it to parse the real version.
const mariadbCompatPrefix = "5.5.5-"

// parseServerVersion turns the output of `SELECT VERSION(), @@version_comment`
// into a core.DBMSVersion, detecting whether the server is MySQL or MariaDB and
// recording the vendor (and raw comment) in Metadata.
//
// Examples:
//
//	"8.0.35"                  -> {8,0,35, vendor=mysql}
//	"8.0.35-0ubuntu0.20.04.1" -> {8,0,35, vendor=mysql}
//	"10.11.5-MariaDB"         -> {10,11,5, vendor=mariadb}
//	"5.5.5-10.11.5-MariaDB"   -> {10,11,5, vendor=mariadb}
func parseServerVersion(versionString, versionComment string) core.DBMSVersion {
	full := strings.TrimSpace(versionString)
	comment := strings.TrimSpace(versionComment)

	vendor := core.DBMSVendorMySQL
	if strings.Contains(strings.ToLower(full+" "+comment), "mariadb") {
		vendor = core.DBMSVendorMariaDB
	}

	numeric := full
	if vendor == core.DBMSVendorMariaDB {
		numeric = strings.TrimPrefix(numeric, mariadbCompatPrefix)
	}
	major, minor, patch := parseVersionTriplet(numeric)

	metadata := map[string]string{core.DBMSVendorKey: vendor}
	if comment != "" {
		metadata[core.DBMSVersionCommentKey] = comment
	}

	return core.DBMSVersion{
		FullString: full,
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		Metadata:   metadata,
	}
}

// parseVersionTriplet extracts the leading major.minor.patch from a version
// string such as "8.0.35-ubuntu" or "10.11.5-MariaDB".
func parseVersionTriplet(s string) (major, minor, patch int) {
	head := s
	if idx := strings.IndexByte(head, '-'); idx >= 0 {
		head = head[:idx]
	}
	parts := strings.Split(head, ".")
	at := func(i int) int {
		if i >= len(parts) {
			return 0
		}
		n, _ := strconv.Atoi(numericPrefix(parts[i]))
		return n
	}
	return at(0), at(1), at(2)
}

// numericPrefix returns the leading run of digits in s.
func numericPrefix(s string) string {
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	return s[:end]
}
