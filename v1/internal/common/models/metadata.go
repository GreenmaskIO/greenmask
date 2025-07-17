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
	StartedAt      time.Time       `yaml:"started_at" json:"started_at"`
	CompletedAt    time.Time       `yaml:"completed_at" json:"completed_at"`
	OriginalSize   int64           `yaml:"original_size" json:"original_size"`
	CompressedSize int64           `yaml:"compressed_size" json:"compressed_size"`
	Transformers   []TableConfig   `yaml:"transformers" json:"transformers"`
	DatabaseSchema []TableMetadata `yaml:"database_schema" json:"database_schema"`
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

	return Metadata{
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		OriginalSize:   originalSize,
		CompressedSize: compressedSize,
		Transformers:   transformers,
		DatabaseSchema: tableMetadata,
	}
}
