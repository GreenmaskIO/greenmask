// Copyright 2023 Greenmask
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

package storage

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/domains"
)

type ObjectSizeStat struct {
	Original   int64
	Compressed int64
}

type Header struct {
	CreationDate    time.Time `json:"creationDate" yaml:"creationDate"`
	DbName          string    `json:"dbName" yaml:"dbName"`
	TocEntriesCount int       `json:"tocEntriesCount" yaml:"tocEntriesCount"`
	DumpVersion     string    `json:"dumpVersion" yaml:"dumpVersion"`
	Format          string    `json:"format" yaml:"format"`
	Integer         uint32    `json:"integer" yaml:"integer"`
	Offset          uint32    `json:"offset" yaml:"offset"`
	DumpedFrom      string    `json:"dumpedFrom" yaml:"dumpedFrom"`
	DumpedBy        string    `json:"dumpedBy" yaml:"dumpedBy"`
	TocFileSize     int64     `json:"tocFileSize" yaml:"tocFileSize"`
	Compression     int32     `json:"compression" yaml:"compression"`
}

type Entry struct {
	DumpId         int32   `json:"dumpId" yaml:"dumpId"`
	DatabaseOid    int32   `json:"databaseOid" yaml:"databaseOid"`
	ObjectOid      int32   `json:"objectOid" yaml:"objectOid"`
	ObjectType     string  `json:"objectType" yaml:"objectType"`
	Schema         string  `json:"schema" yaml:"schema"`
	Name           string  `json:"name" yaml:"name"`
	Owner          string  `json:"owner" yaml:"owner"`
	Section        string  `json:"section" yaml:"section"`
	OriginalSize   int64   `json:"originalSize" yaml:"originalSize"`
	CompressedSize int64   `json:"compressedSize" yaml:"compressedSize"`
	FileName       string  `json:"fileName" yaml:"fileName"`
	Dependencies   []int32 `json:"dependencies" yaml:"dependencies"`
}

type Metadata struct {
	StartedAt      time.Time              `yaml:"startedAt" json:"startedAt"`
	CompletedAt    time.Time              `yaml:"completedAt" json:"completedAt"`
	OriginalSize   int64                  `yaml:"originalSize" json:"originalSize"`
	CompressedSize int64                  `yaml:"compressedSize" json:"compressedSize"`
	Transformers   []*domains.Table       `yaml:"transformers" json:"transformers"`
	DatabaseSchema toolkit.DatabaseSchema `yaml:"database_schema" json:"database_schema"`
	Header         Header                 `yaml:"header" json:"header"`
	Entries        []*Entry               `yaml:"entries" json:"entries"`
}

func NewMetadata(
	tocObj *toc.Toc, tocFileSize int64, startedAt,
	completedAt time.Time, transformers []*domains.Table,
	stats map[int32]ObjectSizeStat, databaseSchema []*toolkit.Table,
) (*Metadata, error) {

	var format string
	switch tocObj.Header.Format {
	case toc.ArchUnknown:
		format = "UNKNOWN"
	case toc.ArchCustom:
		format = "CUSTOM"
	case toc.ArchTar:
		format = "TAR"
	case toc.ArchNull:
		format = "NULL"
	case toc.ArchDirectory:
		format = "DIRECTORY"
	default:
		return nil, fmt.Errorf("unknown archive type %d", tocObj.Header.Format)
	}

	var totalCompressedSize, totalOriginalSize int64

	entriesDto := make([]*Entry, 0, len(tocObj.Entries))
	for _, entry := range tocObj.Entries {

		var objectType, schema, name, owner, fileName string

		if entry.Desc != nil {
			objectType = *entry.Desc
		}

		if entry.Namespace != nil {
			schema = *entry.Namespace
		}

		if entry.Tag != nil {
			name = *entry.Tag
		}

		if entry.Owner != nil {
			owner = *entry.Owner
		}

		if entry.FileName != nil {
			fileName = *entry.FileName
		}

		var objCompressedSize, objOriginalSize int64
		if entry.Section == toc.SectionData && *entry.Desc == toc.TableDataDesc {
			s := stats[entry.DumpId]
			objCompressedSize = s.Compressed
			objOriginalSize = s.Original
			totalCompressedSize += s.Compressed
			totalOriginalSize += s.Original
		}

		section, ok := toc.SectionMap[entry.Section]
		if !ok {
			log.Warn().
				Msgf("unknown section with number: %d", entry.Section)
		}

		entriesDto = append(
			entriesDto, &Entry{
				DumpId:         entry.DumpId,
				DatabaseOid:    int32(entry.CatalogId.Oid),
				ObjectOid:      int32(entry.CatalogId.TableOid),
				ObjectType:     objectType,
				Schema:         schema,
				Name:           name,
				Owner:          owner,
				FileName:       fileName,
				Dependencies:   entry.Dependencies,
				OriginalSize:   objOriginalSize,
				CompressedSize: objCompressedSize,
				Section:        section,
			},
		)
	}

	totalOriginalSize += tocFileSize
	totalCompressedSize += tocFileSize

	return &Metadata{
		OriginalSize:   totalOriginalSize,
		CompressedSize: totalCompressedSize,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		Transformers:   transformers,
		DatabaseSchema: databaseSchema,
		Header: Header{
			CreationDate:    tocObj.Header.CrtmDateTime.Time(),
			DbName:          *tocObj.Header.ArchDbName,
			TocEntriesCount: len(entriesDto),
			DumpVersion:     *tocObj.Header.ArchiveDumpVersion,
			Format:          format,
			Integer:         tocObj.Header.IntSize,
			Offset:          tocObj.Header.OffSize,
			DumpedFrom:      *tocObj.Header.ArchiveRemoteVersion,
			DumpedBy:        *tocObj.Header.ArchiveDumpVersion,
			TocFileSize:     tocFileSize,
			Compression:     tocObj.Header.CompressionSpec.Level,
		},
		Entries: entriesDto,
	}, nil
}
