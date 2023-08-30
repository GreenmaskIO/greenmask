package toclib

import (
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

// TODO: You need to create blobs.toc file like here:
//
//cat pg_dump/data/blobs.toc
//24767 blob_24767.dat
//24768 blob_24768.dat
//24769 blob_24769.dat

var LargeObjectDesc = "BLOBS"

type LargeObjects struct {
	DumpId       DumpId
	Dependencies []int32
}

func (lo *LargeObjects) GetTocEntry() (*toc.Entry, error) {

	fileName := "blobs.toc"

	return &toc.Entry{
		CatalogId: toc.CatalogId{
			Oid:      0,
			TableOid: 0,
		},
		DumpId:       int32(lo.DumpId),
		Section:      toc.SectionData,
		Tag:          &LargeObjectDesc,
		Desc:         &LargeObjectDesc,
		Dependencies: lo.Dependencies,
		NDeps:        int32(len(lo.Dependencies)),
		FileName:     &fileName,
	}, nil
}
