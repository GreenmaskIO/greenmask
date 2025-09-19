package models

import (
	"errors"
	"fmt"
	"time"
)

// TaskID - global unique identifier for objects the object that are stored in the system.
type TaskID int

type Engine string

var (
	EngineMysql      Engine = "mysql"
	EnginePostgresql Engine = "postgresql"
)

var errUnknownEngine = errors.New("unknown engine")

func (m Engine) Validate() error {
	return fmt.Errorf("engine '%s': %w", m, errUnknownEngine)
}

type RestorationItem struct {
	TaskID           TaskID     `json:"task_id"`
	Filename         string     `json:"filename"`
	Engine           Engine     `json:"engine"`
	ObjectKind       ObjectKind `json:"object_kind"`
	ObjectID         ObjectID   `json:"object_id"`
	ObjectDefinition []byte     `json:"object_definition"`
	RecordCount      int64      `json:"record_count"`
}

type RestorationContext struct {
	HasTopologicalOrder bool                `json:"has_topological_order"`
	RestorationOrder    []TaskID            `json:"restoration_order"`
	TaskDependencies    map[TaskID][]TaskID `json:"dependencies"`
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

type Metadata struct {
	Engine                string                  `yaml:"engine" json:"engine"`
	StartedAt             time.Time               `yaml:"started_at" json:"started_at"`
	CompletedAt           time.Time               `yaml:"completed_at" json:"completed_at"`
	OriginalSize          int64                   `yaml:"original_size" json:"original_size"`
	CompressedSize        int64                   `yaml:"compressed_size" json:"compressed_size"`
	Transformers          []TableConfig           `yaml:"transformers" json:"transformers"`
	DatabaseSchema        []Table                 `yaml:"database_schema" json:"database_schema"`
	KindsTopologicalOrder map[ObjectKind][]TaskID `yaml:"kinds_topological_order" json:"kinds_topological_order"`
	DumpStat              DumpStat                `yaml:"dump_stat" json:"dump_stat"`
}

func NewMetadata(
	engine string,
	dumpStat DumpStat,
	startedAt time.Time,
	completedAt time.Time,
	transformers []TableConfig,
	databaseSchema []Table,
) Metadata {
	var originalSize, compressedSize int64
	for _, stat := range dumpStat.TaskStats {
		originalSize += stat.ObjectStat.OriginalSize
		compressedSize += stat.ObjectStat.CompressedSize
	}

	return Metadata{
		Engine:         engine,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		OriginalSize:   originalSize,
		CompressedSize: compressedSize,
		Transformers:   transformers,
		DatabaseSchema: databaseSchema,
		DumpStat:       dumpStat,
	}
}
