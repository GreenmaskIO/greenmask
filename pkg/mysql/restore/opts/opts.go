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

// Package opts holds the parameter structs shared between the MySQL restore
// connection config and the factory-level restorers. Keeping them in a neutral
// package breaks the otherwise-circular dependency between the connconfig and
// factory packages.
package opts

import commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"

// TableRestoreOpts bundles the data-restore parameters extracted from the
// connection config and passed to InsertRestoreWriter / CsvRestoreWriter.
type TableRestoreOpts struct {
	PrintWarnings           bool
	MaxFetchWarnings        int
	DisableForeignKeyChecks bool
	DisableUniqueChecks     bool
	InsertIgnore            bool
	InsertReplace           bool
	MaxInsertStatementSize  int
	RemapDatabase           map[string]string
}

// SchemaRestoreOpts bundles the schema-restore parameters extracted from the
// connection config and passed to MysqlSchemaRestorer.
type SchemaRestoreOpts struct {
	SSL            commonconfig.SSLOpts
	CreateDatabase bool
	IfNotExists    bool
	RemapDatabase  map[string]string
}
