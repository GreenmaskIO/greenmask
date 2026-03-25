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
	"errors"
	"fmt"
	"time"
)

type ObjectID int

type ObjectKind string

const (
	ObjectKindTable ObjectKind = "table"
)

type Compression string

var ErrModelValidation = errors.New("model validation error")

const (
	CompressionNone  Compression = "none"
	CompressionGzip  Compression = "gzip"
	CompressionPgzip Compression = "pgzip"
)

type DumpFormat string

const (
	DumpFormatCsv    DumpFormat = "csv"
	DumpFormatInsert DumpFormat = "insert"
)

func (c Compression) Validate() error {
	switch c {
	case CompressionNone, CompressionGzip, CompressionPgzip:
		return nil
	default:
		return fmt.Errorf("value = '%s': %w", string(c), ErrModelValidation)
	}
}

func (c Compression) IsEnabled() bool {
	return c != CompressionNone
}

func (c Compression) IsPgzip() bool {
	return c == CompressionPgzip
}

func (f DumpFormat) Validate() error {
	switch f {
	case DumpFormatCsv, DumpFormatInsert:
		return nil
	default:
		return fmt.Errorf("value = '%s': %w", string(f), ErrModelValidation)
	}
}

type DumpStat struct {
	RestorationContext RestorationContext                 `json:"restoration_context"`
	RestorationItems   map[TaskID]RestorationItem         `json:"restoration_items"`
	TaskStats          map[TaskID]TaskStat                `json:"task_stats"`
	TaskID2ObjectID    map[ObjectKind]map[TaskID]ObjectID `json:"task_id_2_object_id"`
	ObjectID2TaskID    map[ObjectKind]map[ObjectID]TaskID `json:"object_id_2_task_id"`
}

type ObjectStat struct {
	Engine          DBMSEngine  `json:"engine"`
	ID              ObjectID    `json:"id"`
	Kind            ObjectKind  `json:"kind"`
	HumanReadableID string      `json:"human_readable_id"`
	OriginalSize    int64       `json:"original_size"`
	CompressedSize  int64       `json:"compressed_size"`
	Filename        string      `json:"filename"`
	Compression     Compression `json:"compression"`
	Format          DumpFormat  `json:"format"`
}

func NewObjectStat(
	engine DBMSEngine,
	kind ObjectKind,
	id ObjectID,
	humanReadableID string,
	size int64,
	compressedSize int64,
	fileName string,
	compression Compression,
	format DumpFormat,
) ObjectStat {
	return ObjectStat{
		Engine:          engine,
		Kind:            kind,
		ID:              id,
		HumanReadableID: humanReadableID,
		OriginalSize:    size,
		CompressedSize:  compressedSize,
		Filename:        fileName,
		Compression:     compression,
		Format:          format,
	}
}

type TaskStat struct {
	ObjectStat  ObjectStat    `json:"object_stat"`
	ID          TaskID        `json:"id"`
	Engine      DBMSEngine    `json:"engine"`
	Duration    time.Duration `json:"duration"`
	DumperType  string        `json:"dumper_type"`
	RecordCount int64         `json:"record_count"`
	// ObjectDefinition - definition of the object in JSON bytes.
	ObjectDefinition []byte `json:"table"`
}

func NewDumpStat(
	taskID TaskID,
	objectStat ObjectStat,
	duration time.Duration,
	dumperType string,
	recordCount int64,
	engine DBMSEngine,
	objectDefinition []byte,
) TaskStat {
	return TaskStat{
		ID:               taskID,
		ObjectStat:       objectStat,
		Duration:         duration,
		DumperType:       dumperType,
		RecordCount:      recordCount,
		Engine:           engine,
		ObjectDefinition: objectDefinition,
	}
}

type DumpedDatabaseSchemaStat struct {
	DatabaseName   string      `json:"database_name"`
	FileName       string      `json:"file_name"`
	Compression    Compression `json:"compression"`
	OriginalSize   int64       `json:"original_size"`
	CompressedSize int64       `json:"compressed_size"`
}
