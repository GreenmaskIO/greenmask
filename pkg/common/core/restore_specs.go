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

// ObjectRestoreSpec is the runtime (in-memory) analogue of ObjectDumpSpec.
//
// It is NOT JSON-serialized. The RestorePlanBuilder constructs it from the
// persisted RestorationItem by calling json.Unmarshal(item.ObjectDefinition)
// into the right engine-specific type and placing the result in Payload.
// Factories receive a fully typed Payload and type-assert it directly.
type ObjectRestoreSpec struct {
	TaskID      TaskID
	Kind        ObjectKind
	Filename    string
	Compression Compression
	Format      DumpFormat
	RecordCount int64
	Payload     any // engine-specific typed struct, never []byte
}

// SchemaRestoreSpec is the runtime analogue of SchemaDumpSpec.
//
// Section mirrors SchemaDumpStat.Section: the dump creates one SchemaDumpStat
// per (database, section) pair, so each SchemaRestoreSpec is either pre-data
// or post-data. The processor partitions the list by Section to sequence calls.
type SchemaRestoreSpec struct {
	Kind    SchemaObjectKind
	Section DumpSection // pre-data | post-data
	Payload any         // engine-specific; built by RestorePlanBuilder
}
