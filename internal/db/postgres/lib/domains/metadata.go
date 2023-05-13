package domains

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

type TocHeader struct {
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

type TocEntry struct {
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
	StartedAt      time.Time   `yaml:"startedAt" json:"startedAt"`
	CompletedAt    time.Time   `yaml:"completedAt" json:"completedAt"`
	OriginalSize   int64       `json:"originalSize" json:"originalSize"`
	CompressedSize int64       `json:"compressedSize" json:"compressedSize"`
	Transformers   []Table     `yaml:"transformers" json:"transformers"`
	Header         TocHeader   `json:"header" json:"header"`
	Entries        []*TocEntry `json:"entries" json:"entries"`
}

func NewMetadata(ahHeader toc.Header, ahEntries []*toc.Entry,
	tocFileSize int64, startedAt, completedAt time.Time,
	transformers []Table,
) (*Metadata, error) {

	var format string
	switch ahHeader.Format {
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
		return nil, fmt.Errorf("unknown archive type %d", ahHeader.Format)
	}

	var totalCompressedSize, totalOriginalSize int64

	entries := make([]*TocEntry, 0)
	for _, ahEntry := range ahEntries {
		if ahEntry.Section == toc.SectionPreData ||
			ahEntry.Section == toc.SectionData ||
			ahEntry.Section == toc.SectionPostData {

			var objectType, schema, name, owner, fileName string

			if ahEntry.Desc != nil {
				objectType = *ahEntry.Desc
			}

			if ahEntry.Namespace != nil {
				schema = *ahEntry.Namespace
			}

			if ahEntry.Tag != nil {
				name = *ahEntry.Tag
			}

			if ahEntry.Owner != nil {
				owner = *ahEntry.Owner
			}

			if ahEntry.FileName != nil {
				fileName = *ahEntry.FileName
			}

			totalCompressedSize += ahEntry.CompressedSize
			totalOriginalSize += ahEntry.OriginalSize

			section, ok := toc.SectionMap[ahEntry.Section]
			if !ok {
				log.Warn().Msgf("unknown section with number: %d", ahEntry.Section)
			}

			entries = append(entries, &TocEntry{
				DumpId:         ahEntry.DumpId,
				DatabaseOid:    int32(ahEntry.CatalogId.Oid),
				ObjectOid:      int32(ahEntry.CatalogId.TableOid),
				ObjectType:     objectType,
				Schema:         schema,
				Name:           name,
				Owner:          owner,
				FileName:       fileName,
				Dependencies:   ahEntry.Dependencies,
				OriginalSize:   ahEntry.OriginalSize,
				CompressedSize: ahEntry.CompressedSize,
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
		Header: TocHeader{
			CreationDate:    ahHeader.CrtmDateTime.Time(),
			DbName:          *ahHeader.ArchDbName,
			TocEntriesCount: len(entries),
			DumpVersion:     *ahHeader.ArchiveDumpVersion,
			Format:          format,
			Integer:         ahHeader.IntSize,
			Offset:          ahHeader.OffSize,
			DumpedFrom:      *ahHeader.ArchiveRemoteVersion,
			DumpedBy:        *ahHeader.ArchiveDumpVersion,
			TocFileSize:     tocFileSize,
			Compression:     ahHeader.CompressionSpec.Level,
		},
		Entries: entries,
	}, nil
}
