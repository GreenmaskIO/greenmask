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
	ExcludeData  bool
}

func (s *Sequence) GetTocEntry() (*toc.Entry, error) {
	isCalled := "true"
	if !s.IsCalled {
		isCalled = "false"
	}
	statement := fmt.Sprintf(`SELECT pg_catalog.setval('"%s"."%s"', %d, %s)`, s.Schema, s.Name, s.LastValue, isCalled)
	fileName := ""

	name := fmt.Sprintf(`"%s"`, s.Name)
	schema := fmt.Sprintf(`"%s"`, s.Schema)
	owner := ""
	if s.Owner != "" {
		owner = fmt.Sprintf(`"%s"`, s.Owner)
	}

	return &toc.Entry{
		CatalogId:    toc.CatalogId{},
		DumpId:       s.DumpId,
		Section:      toc.SectionData,
		HadDumper:    0,
		Tag:          &name,
		Namespace:    &schema,
		Owner:        &owner,
		Desc:         &SequenceSetDesc,
		Defn:         &statement,
		Dependencies: s.Dependencies,
		NDeps:        int32(len(s.Dependencies)),
		FileName:     &fileName,
	}, nil
}
