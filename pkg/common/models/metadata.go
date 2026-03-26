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

package models

import (
	"time"
)

// TaskID - global unique identifier for objects the object that are stored in the system.
type TaskID int

type RestorationItem struct {
	TaskID           TaskID      `json:"task_id"`
	Filename         string      `json:"filename"`
	Engine           DBMSEngine  `json:"engine"`
	ObjectKind       ObjectKind  `json:"object_kind"`
	ObjectID         ObjectID    `json:"object_id"`
	ObjectDefinition []byte      `json:"object_definition"`
	RecordCount      int64       `json:"record_count"`
	Compression      Compression `json:"compression"`
}

type RestorationContext struct {
	HasTopologicalOrder      bool                `json:"has_topological_order"`
	RestorationOrder         []TaskID            `json:"restoration_order"`
	TaskDependencies         map[TaskID][]TaskID `json:"dependencies"`
	TableIDToAffectedColumns map[ObjectID][]int  `json:"table_id_to_affected_columns"`
}

type TableMetadata struct {
	ID             int      `json:"id"`
	Schema         string   `json:"schema"`
	Name           string   `json:"name"`
	Columns        []Column `json:"columns"`
	PrimaryKey     []string `json:"primary_key"`
	OriginalSize   int64    `json:"original_size"`
	CompressedSize int64    `json:"compressed_size"`
}

func (m *TableMetadata) ToTable() Table {
	return Table{
		ID:         m.ID,
		Schema:     m.Schema,
		Name:       m.Name,
		Columns:    m.Columns,
		PrimaryKey: m.PrimaryKey,
	}
}

type DataSectionEntry struct {
	ID             string     `json:"id"`
	Kind           ObjectKind `json:"kind"`
	FileName       string     `json:"file_name"`
	RecordCount    int64      `json:"record_count"`
	ObjectFullName string     `json:"object_full_name"`
	ObjectID       ObjectID   `json:"object_id"`
	TaskID         TaskID     `json:"task_id"`
}

type DataDumpMetadata struct {
	Transformers          []TableConfig           `yaml:"transformers" json:"transformers"`
	KindsTopologicalOrder map[ObjectKind][]TaskID `yaml:"kinds_topological_order" json:"kinds_topological_order"`
	DumpStat              DumpStat                `yaml:"dump_stat" json:"dump_stat"`
	OriginalSize          int64                   `yaml:"original_size" json:"original_size"`
	CompressedSize        int64                   `yaml:"compressed_size" json:"compressed_size"`
}

func NewDataDumpMetadata(
	transformers []TableConfig,
	kindsTopologicalOrder map[ObjectKind][]TaskID,
	dumpStat DumpStat,
) *DataDumpMetadata {
	if len(dumpStat.TaskStats) == 0 {
		return nil
	}
	var originalSize, compressedSize int64
	for _, stat := range dumpStat.TaskStats {
		originalSize += stat.ObjectStat.OriginalSize
		compressedSize += stat.ObjectStat.CompressedSize
	}
	return &DataDumpMetadata{
		Transformers:          transformers,
		KindsTopologicalOrder: kindsTopologicalOrder,
		DumpStat:              dumpStat,
		OriginalSize:          originalSize,
		CompressedSize:        compressedSize,
	}
}

type SchemaDumpMetadata struct {
	DumpedDatabaseSchema []DumpedDatabaseSchemaStat `yaml:"dumped_database_schema" json:"dumped_database_schema"`
	OriginalSize         int64                      `yaml:"original_size" json:"original_size"`
	CompressedSize       int64                      `yaml:"compressed_size" json:"compressed_size"`
}

func NewSchemaDumpMetadata(
	dumpedDatabaseSchema []DumpedDatabaseSchemaStat,
) *SchemaDumpMetadata {
	if len(dumpedDatabaseSchema) == 0 {
		return nil
	}
	var originalSize, compressedSize int64
	for _, schemaStat := range dumpedDatabaseSchema {
		originalSize += schemaStat.OriginalSize
		compressedSize += schemaStat.CompressedSize
	}
	return &SchemaDumpMetadata{
		DumpedDatabaseSchema: dumpedDatabaseSchema,
		OriginalSize:         originalSize,
		CompressedSize:       compressedSize,
	}
}

type Metadata struct {
	Engine         DBMSEngine          `yaml:"engine" json:"engine"`
	StartedAt      time.Time           `yaml:"started_at" json:"started_at"`
	CompletedAt    time.Time           `yaml:"completed_at" json:"completed_at"`
	OriginalSize   int64               `yaml:"original_size" json:"original_size"`
	CompressedSize int64               `yaml:"compressed_size" json:"compressed_size"`
	Description    string              `yaml:"description" json:"description"`
	Tags           []string            `yaml:"tags" json:"tags"`
	Introspection  []Table             `yaml:"introspection" json:"introspection"`
	DataDump       *DataDumpMetadata   `yaml:"data_dump" json:"data_dump"`
	SchemaDump     *SchemaDumpMetadata `yaml:"schema_dump" json:"schema_dump"`
	Databases      []string            `yaml:"databases" json:"databases"`
}

func NewMetadata(
	engine DBMSEngine,
	startedAt time.Time,
	completedAt time.Time,
	description string,
	tags []string,
	introspection []Table,
	dataDump *DataDumpMetadata,
	schemaDump *SchemaDumpMetadata,
	databases []string,
) Metadata {
	var originalSize, compressedSize int64

	if dataDump != nil {
		originalSize += dataDump.OriginalSize
		compressedSize += dataDump.CompressedSize
	}

	if schemaDump != nil {
		originalSize += schemaDump.OriginalSize
		compressedSize += schemaDump.CompressedSize
	}

	return Metadata{
		Engine:         engine,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		OriginalSize:   originalSize,
		CompressedSize: compressedSize,
		Tags:           tags,
		Description:    description,
		Introspection:  introspection,
		DataDump:       dataDump,
		SchemaDump:     schemaDump,
		Databases:      databases,
	}
}
