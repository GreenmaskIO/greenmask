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

// Package version holds MySQL/MariaDB version parsing shared by the dump and
// restore pipelines: SQL VERSION() parsing (ParseServerVersion) and vendor CLI
// "--version" output parsing (ParseUtilityVersion).
package version

import (
	"regexp"
	"strconv"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// mariadbCompatPrefix is the "replication version" prefix MariaDB (>= 10) prepends
// to VERSION() over the legacy protocol so old MySQL clients tolerate the major
// version. We strip it to parse the real version.
const mariadbCompatPrefix = "5.5.5-"

// utilityVersionRe extracts the "Ver X.Y.Z…" token mysql/mysqldump/mariadb
// print in their "--version" output, e.g.:
//
//	"mysqldump  Ver 8.0.35 for Linux on x86_64 (MySQL Community Server - GPL)"
//	"mysql  Ver 14.14 Distrib 5.7.42, for Linux (x86_64) using EditLine wrapper"
//	"mysqldump  Ver 10.11.5-MariaDB for debian-linux-gnu on x86_64"
var utilityVersionRe = regexp.MustCompile(`Ver\s+([^\s,]+)`)

// ParseServerVersion turns the output of `SELECT VERSION(), @@version_comment`
// into a core.DBMSVersion, detecting whether the server is MySQL or MariaDB and
// recording the vendor (and raw comment) in Metadata.
//
// Examples:
//
//	"8.0.35"                  -> {8,0,35, vendor=mysql}
//	"8.0.35-0ubuntu0.20.04.1" -> {8,0,35, vendor=mysql}
//	"10.11.5-MariaDB"         -> {10,11,5, vendor=mariadb}
//	"5.5.5-10.11.5-MariaDB"   -> {10,11,5, vendor=mariadb}
//	"8.0.35-27" / "Percona Server (GPL)" -> {8,0,35, vendor=percona}
func ParseServerVersion(versionString, versionComment string) core.DBMSVersion {
	full := strings.TrimSpace(versionString)
	comment := strings.TrimSpace(versionComment)

	// Percona reports a MySQL-style VERSION() but identifies itself in
	// @@version_comment ("Percona Server (GPL)..."); MariaDB tags both fields.
	lower := strings.ToLower(full + " " + comment)
	vendor := core.DBMSVendorMySQL
	switch {
	case strings.Contains(lower, "mariadb"):
		vendor = core.DBMSVendorMariaDB
	case strings.Contains(lower, "percona"):
		vendor = core.DBMSVendorPercona
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

// ParseUtility extracts the utility's self-reported name (the leading token) and
// version token from a vendor CLI "--version" output, e.g.:
//
//	"mysqldump  Ver 8.0.35 for Linux on x86_64 (MySQL Community Server - GPL)"
//	  -> ("mysqldump", "8.0.35")
//
// Either return value is "" when it cannot be determined. The name is taken from
// the output rather than the invoked executable, which may be a path.
func ParseUtility(raw string) (name, version string) {
	if fields := strings.Fields(raw); len(fields) > 0 {
		name = fields[0]
	}
	return name, ParseUtilityVersion(raw)
}

// ParseUtilityVersion extracts the version token from a vendor CLI "--version"
// output (the value following "Ver"). Returns "" when no token is found.
func ParseUtilityVersion(raw string) string {
	m := utilityVersionRe.FindStringSubmatch(raw)
	if len(m) < 2 {
		return ""
	}
	return m[1]
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
		n, err := strconv.Atoi(numericPrefix(parts[i]))
		if err != nil {
			return 0
		}
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
