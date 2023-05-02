package domains

import (
	"fmt"
	"time"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

type TocHeader struct {
	CreationDate    time.Time `json:"creationDate"`
	DbName          string    `json:"dbName"`
	TocEntriesCount int       `json:"tocEntriesCount"`
	DumpVersion     string    `json:"dumpVersion"`
	Format          string    `json:"format"`
	Integer         uint32    `json:"integer"`
	Offset          uint32    `json:"offset"`
	DumpedFrom      string    `json:"dumpedFrom"`
	DumpedBy        string    `json:"dumpedBy"`
	TocFileSize     int64     `json:"tocFileSize"`
}

type TocEntry struct {
	DumpId         int32   `json:"dumpId"`
	DatabaseOid    int32   `json:"databaseOid"`
	ObjectOid      int32   `json:"objectOid"`
	ObjectType     string  `json:"objectType"`
	Schema         string  `json:"schema"`
	Name           string  `json:"name"`
	Owner          string  `json:"owner"`
	OriginalSize   int64   `json:"originalSize"`
	CompressedSize int64   `json:"compressedSize"`
	FileName       string  `json:"fileName"`
	Dependencies   []int32 `json:"dependencies"`
}

type Metadata struct {
	StartedAt      time.Time   `yaml:"startedAt"`
	CompletedAt    time.Time   `yaml:"completedAt"`
	OriginalSize   int64       `json:"originalSize"`
	CompressedSize int64       `json:"compressedSize"`
	Header         TocHeader   `json:"header"`
	Entries        []*TocEntry `json:"entries"`
}

func NewMetadata(ahHeader toc.Header, ahEntries []*toc.Entry, tocFileSize int64, startedAt, completedAt time.Time) (*Metadata, error) {

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
		},
		Entries: entries,
	}, nil
}
