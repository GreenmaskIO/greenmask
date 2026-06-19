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
	"context"
)

type ObjectDumper interface {
	// Dump executes the object dump. The runtime resources are injected at
	// execution time: session provides DBMS connections (acquired via
	// DumpSession.RunWithEngineResource) and st is the destination storage.
	// Both may be nil for dumpers whose resources were bound at construction
	// time (the legacy task-producer path).
	Dump(ctx context.Context, session DumpSession, st Storager) (ObjectDumpStat, error)
	DebugInfo() string
	Meta() map[string]any
}

type SchemaDumper interface {
	// Dump executes the schema dump. Like ObjectDumper.Dump, the runtime
	// resources are injected at execution time: conn carries the connection
	// attributes the vendor CLI (mysqldump, pg_dump) needs — transformed into
	// env vars and parameters by the dumper — and st is the destination storage.
	Dump(ctx context.Context, conn ConnectionConfigurer, st Storager) (SchemaDumpStat, error)
	DebugInfo() string
	Meta() map[string]any
}
