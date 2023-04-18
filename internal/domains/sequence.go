package domains

import (
	"fmt"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

var SequenceSetDesc = "SEQUENCE SET"

type Sequence struct {
	Name         string
	Schema       string
	Oid          int
	Owner        string
	DumpId       int32
	LastValue    int64
	Dependencies []int32
	IsCalled     bool
}

func (s *Sequence) GetTocEntry() (*toc.Entry, error) {
	isCalled := "true"
	if !s.IsCalled {
		isCalled = "false"
	}
	statement := fmt.Sprintf(`SELECT pg_catalog.setval('%s.%s', %d, %s)`, s.Schema, s.Name, s.LastValue, isCalled)
	fileName := ""

	return &toc.Entry{
		CatalogId:    toc.CatalogId{},
		DumpId:       s.DumpId,
		Section:      toc.SectionData,
		HadDumper:    0,
		Tag:          &s.Name,
		Namespace:    &s.Schema,
		Owner:        &s.Owner,
		Desc:         &SequenceSetDesc,
		Defn:         &statement,
		Dependencies: s.Dependencies,
		NDeps:        int32(len(s.Dependencies)),
		FileName:     &fileName,
	}, nil
}
