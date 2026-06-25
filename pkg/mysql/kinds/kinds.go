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

// Package kinds owns the MySQL engine's object/schema taxonomy. These constants
// used to live in pkg/common/core, which forced core to enumerate every engine's
// kinds. Keeping them here means core stays engine-agnostic: the generic registry
// looks kinds up by value, so a new engine adds its kinds in its own package
// without touching the shared core surface.
package kinds

import (
	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

const (
	// ObjectKindTable is the kind of a MySQL table dumped by greenmask itself.
	ObjectKindTable core.ObjectKind = "mysql.table"
	// ObjectKindDatabase is the kind of a MySQL database object.
	ObjectKindDatabase core.ObjectKind = "mysql.database"

	// SchemaObjectKindDatabase selects the mysqldump-based schema dumper for a
	// MySQL database.
	SchemaObjectKindDatabase core.SchemaObjectKind = "mysql.database"
	// SchemaObjectKindSchema is the MySQL schema-section kind.
	SchemaObjectKindSchema core.SchemaObjectKind = "mysql.schema"
)
