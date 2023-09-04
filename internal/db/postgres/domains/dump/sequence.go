package dump

import (
	"fmt"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/toc"
)

type Sequence struct {
	Oid          toc.Oid
	Schema       string
	Name         string
	Owner        string
	IsCalled     bool
	DumpId       int32
	Dependencies []int32
	ExcludeData  bool
	LastValue    int64
}

func (s *Sequence) SetDumpId(dumpId int32) {
	if dumpId == 0 {
		panic("dumpId cannot be 0")
	}
	s.DumpId = dumpId
}

func (s *Sequence) Entry() (*toc.Entry, error) {
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
		// TODO: CatalogId is setting as 0. Ensure that it's fine
		CatalogId:    toc.CatalogId{},
		DumpId:       s.DumpId,
		Section:      toc.SectionData,
		HadDumper:    0,
		Tag:          &name,
		Namespace:    &schema,
		Owner:        &owner,
		Desc:         &toc.SequenceSetDesc,
		Defn:         &statement,
		Dependencies: s.Dependencies,
		NDeps:        int32(len(s.Dependencies)),
		FileName:     &fileName,
	}, nil
}
