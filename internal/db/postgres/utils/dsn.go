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
	"slices"
	"strings"
)

// IsConnString reports whether a --dbname value is a connection string rather than a plain
// database name. libpq treats it as one when it starts with postgresql:// or postgres://, or
// contains =.
func IsConnString(dbName string) bool {
	return strings.HasPrefix(dbName, "postgresql://") || strings.HasPrefix(dbName, "postgres://") ||
		strings.Contains(dbName, "=")
}

// HideDbNameInArgs returns a copy of pg_dump or pg_restore arguments with the --dbname value
// hidden when it is a connection string, since it can hold a password. A plain database name is
// kept, and the arguments passed to the command are not changed.
func HideDbNameInArgs(args []string) []string {
	res := slices.Clone(args)
	for i := 0; i < len(res)-1; i++ {
		if res[i] == "--dbname" && IsConnString(res[i+1]) {
			res[i+1] = "***"
		}
	}
	return res
}
