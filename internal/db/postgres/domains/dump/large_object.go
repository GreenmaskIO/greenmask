package dump

import (
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"
)

type LargeObject struct {
	Name           string
	DumpId         int32
	Dependencies   []int32
	OriginalSize   int64
	CompressedSize int64
}

func (lo *LargeObject) SetDumpId(dumpId int32) {
	if dumpId == 0 {
		panic("dumpId cannot be 0")
	}
	lo.DumpId = dumpId
}

func (lo *LargeObject) Entry() (*toc.Entry, error) {

	fileName := "blobs.toc"

	return &toc.Entry{
		CatalogId: toc.CatalogId{
			Oid:      0,
			TableOid: 0,
		},
		DumpId:       lo.DumpId,
		Section:      toc.SectionData,
		Tag:          &lo.Name,
		Desc:         &toc.LargeObjectDesc,
		Dependencies: lo.Dependencies,
		NDeps:        int32(len(lo.Dependencies)),
		FileName:     &fileName,
	}, nil
}
