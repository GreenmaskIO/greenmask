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

import "context"

// RestoreRowReader reads rows from a dump file in storage.
//
// Each ReadRow call returns the raw encoded row as it appears in the dump file.
// For INSERT format this is the values tuple bytes e.g. (1,'foo',NULL).
// Returns ErrEndOfStream when the file is exhausted.
type RestoreRowReader interface {
	Open(ctx context.Context, st Storager) error
	ReadRow(ctx context.Context) ([]byte, error) // ErrEndOfStream when done
	Close(ctx context.Context) error
	DebugInfo() map[string]any
}

// RestoreRowWriter writes rows to the target DB via session.RunWithOperationalDB.
//
// row is the raw bytes produced by RestoreRowReader — no re-encoding required.
// For INSERT format row is the values tuple e.g. (1,'foo',NULL).
type RestoreRowWriter interface {
	Open(ctx context.Context, session DatabaseSession, conn ConnectionConfigurer) error
	WriteRow(ctx context.Context, row []byte) error
	Close(ctx context.Context) error
}
