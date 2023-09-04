package storage

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/config"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/dump"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/toc"
)

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
	StartedAt      time.Time       `yaml:"startedAt" json:"startedAt"`
	CompletedAt    time.Time       `yaml:"completedAt" json:"completedAt"`
	OriginalSize   int64           `json:"originalSize" json:"originalSize"`
	CompressedSize int64           `json:"compressedSize" json:"compressedSize"`
	Transformers   []*config.Table `yaml:"transformers" json:"transformers"`
	Header         Header          `json:"header" json:"header"`
	Entries        []*Entry        `json:"entries" json:"entries"`
}

func NewMetadata(
	header *toc.Header,
	entryProducers []toc.EntryProducer,
	tocFileSize int64, startedAt,
	completedAt time.Time, transformers []*config.Table,
) (*Metadata, error) {

	var format string
	switch header.Format {
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
		return nil, fmt.Errorf("unknown archive type %d", header.Format)
	}

	var totalCompressedSize, totalOriginalSize int64

	entriesDto := make([]*Entry, 0, len(entryProducers))
	for _, ep := range entryProducers {
		entry, err := ep.Entry()
		if err != nil {
			return nil, fmt.Errorf("error producing toc entry: %s", err)
		}
		if entry.Section == toc.SectionPreData ||
			entry.Section == toc.SectionData ||
			entry.Section == toc.SectionPostData {

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
				table, ok := ep.(*dump.Table)
				if !ok {
					return nil, fmt.Errorf("unable to cast to dump.Table")
				}
				objCompressedSize = table.CompressedSize
				objOriginalSize = table.OriginalSize
				totalCompressedSize += table.CompressedSize
				totalOriginalSize += table.OriginalSize
			}

			section, ok := toc.SectionMap[entry.Section]
			if !ok {
				log.Warn().
					Msgf("unknown section with number: %d", entry.Section)
			}

			entriesDto = append(entriesDto, &Entry{
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
			})

		}
	}

	totalOriginalSize += tocFileSize
	totalCompressedSize += tocFileSize

	return &Metadata{
		OriginalSize:   totalOriginalSize,
		CompressedSize: totalCompressedSize,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		Transformers:   transformers,
		Header: Header{
			CreationDate:    header.CrtmDateTime.Time(),
			DbName:          *header.ArchDbName,
			TocEntriesCount: len(entriesDto),
			DumpVersion:     *header.ArchiveDumpVersion,
			Format:          format,
			Integer:         header.IntSize,
			Offset:          header.OffSize,
			DumpedFrom:      *header.ArchiveRemoteVersion,
			DumpedBy:        *header.ArchiveDumpVersion,
			TocFileSize:     tocFileSize,
			Compression:     header.CompressionSpec.Level,
		},
		Entries: entriesDto,
	}, nil
}
