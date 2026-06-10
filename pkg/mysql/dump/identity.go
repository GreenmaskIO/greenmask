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
	"slices"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// mysqlTableIdentity builds the stable identity of a MySQL table
// (kind=mysql.table, scoped by database and table name).
func mysqlTableIdentity(schema, name string) core.EntityIdentity {
	return core.EntityIdentity{
		Kind:      core.EntityKindMysqlTable,
		NameParts: []string{"database", "table"},
		NameValues: map[string]string{
			"database": schema,
			"table":    name,
		},
	}
}

// MySQLSourceDatabasePayload is the MySQL-specific source payload carried on
// core.SourceSpec.Payload.
type MySQLSourceDatabasePayload struct {
	// Databases are the deduplicated, sorted databases the dump covers.
	Databases []string
	// Version is the source server version (and vendor, in its Metadata).
	Version core.DBMSVersion
	// VendorParameters carries MySQL-specific, run-specific source parameters
	// (e.g. gtid, binlog file/position, snapshot id) captured from the dump
	// session. They are not available at context-build time and are populated by
	// the session-aware stage.
	VendorParameters map[string]any
}

// mysqlSourceSpec builds the MySQL source spec (identity + payload) from the
// in-scope databases and the server version. The databases are deduplicated and
// sorted so the identity (and the snapshot key derived from it) is stable across
// runs.
func mysqlSourceSpec(databases []string, version core.DBMSVersion) core.SourceSpec {
	seen := make(map[string]struct{}, len(databases))
	distinct := make([]string, 0, len(databases))
	for _, db := range databases {
		if _, ok := seen[db]; ok {
			continue
		}
		seen[db] = struct{}{}
		distinct = append(distinct, db)
	}
	slices.Sort(distinct)

	return core.SourceSpec{
		Engine: core.DBMSEngineMySQL,
		Identity: core.EntityIdentity{
			Kind:       core.EntityKindMysqlServer,
			NameParts:  []string{"databases"},
			NameValues: map[string]string{"databases": strings.Join(distinct, ",")},
		},
		Payload: MySQLSourceDatabasePayload{
			Databases: distinct,
			Version:   version,
		},
	}
}
