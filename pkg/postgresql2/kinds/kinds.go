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

// Package kinds owns the PostgreSQL engine's object/schema taxonomy. These
// constants used to live in pkg/common/core, which forced core to enumerate every
// engine's kinds. Keeping them here means core stays engine-agnostic: the generic
// registry looks kinds up by value, so a new engine adds its kinds in its own
// package without touching the shared core surface.
package kinds

import (
	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

const (
	// ObjectKindTable is the kind of a PostgreSQL table dumped by greenmask itself.
	ObjectKindTable core.ObjectKind = "pg.table"
	// ObjectKindSequence is the kind of a PostgreSQL sequence.
	ObjectKindSequence core.ObjectKind = "pg.sequence"
	// ObjectKindBlobs is the kind of PostgreSQL large objects.
	ObjectKindBlobs core.ObjectKind = "pg.large_objects"
	// ObjectKindSchema is the PostgreSQL schema object kind.
	ObjectKindSchema core.ObjectKind = "pg.schema"
	// ObjectKindDatabase is the PostgreSQL database object kind.
	ObjectKindDatabase core.ObjectKind = "pg.database"

	// SchemaObjectKindDatabase selects the pg_dump-based schema dumper for a
	// PostgreSQL database.
	SchemaObjectKindDatabase core.SchemaObjectKind = "pg.database"
	// SchemaObjectKindSchema is the PostgreSQL schema-section kind.
	SchemaObjectKindSchema core.SchemaObjectKind = "pg.schema"
)
