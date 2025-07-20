package models

import (
	"time"
)

type TableMetadata struct {
	ID             int      `json:"id"`
	Schema         string   `json:"schema"`
	Name           string   `json:"name"`
	Columns        []Column `json:"columns"`
	PrimaryKey     []string `json:"primary_key"`
	OriginalSize   int64    `json:"original_size"`
	CompressedSize int64    `json:"compressed_size"`
}

type DataSectionEntry struct {
	ID       string     `json:"id"`
	Kind     ObjectKind `json:"kind"`
	FileName string     `json:"file_name"`
}

func NewTableMetadata(stat DumpStat, table Table) TableMetadata {
	return TableMetadata{
		ID:             table.ID,
		Schema:         table.Schema,
		Name:           table.Name,
		Columns:        table.Columns,
		PrimaryKey:     table.PrimaryKey,
		OriginalSize:   stat.OriginalSize,
		CompressedSize: stat.CompressedSize,
	}
}

type Metadata struct {
	StartedAt      time.Time          `yaml:"started_at" json:"started_at"`
	CompletedAt    time.Time          `yaml:"completed_at" json:"completed_at"`
	OriginalSize   int64              `yaml:"original_size" json:"original_size"`
	CompressedSize int64              `yaml:"compressed_size" json:"compressed_size"`
	Transformers   []TableConfig      `yaml:"transformers" json:"transformers"`
	DatabaseSchema []TableMetadata    `yaml:"database_schema" json:"database_schema"`
	DataSection    []DataSectionEntry `yaml:"data_section" json:"data_section"`
}

func NewMetadata(
	stats []DumpStat,
	startedAt time.Time,
	completedAt time.Time,
	transformers []TableConfig,
	databaseSchema []Table,
) Metadata {
	var originalSize, compressedSize int64
	for _, stat := range stats {
		originalSize += stat.OriginalSize
		compressedSize += stat.CompressedSize
	}
	tableMetadata := make([]TableMetadata, 0, len(databaseSchema))
	for i, table := range databaseSchema {
		tableMetadata = append(tableMetadata, NewTableMetadata(stats[i], table))
	}
	dataSection := make([]DataSectionEntry, len(databaseSchema))
	for i := range stats {
		dataSection[i] = DataSectionEntry{
			ID:       stats[i].ID,
			Kind:     stats[i].Kind,
			FileName: stats[i].FileName,
		}
	}

	return Metadata{
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		OriginalSize:   originalSize,
		CompressedSize: compressedSize,
		Transformers:   transformers,
		DatabaseSchema: tableMetadata,
		DataSection:    dataSection,
	}
}
