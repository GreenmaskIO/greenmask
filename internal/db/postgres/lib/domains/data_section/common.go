package data_section

import (
	"sync/atomic"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

type TocMaker interface {
	GetTocEntry() (*toc.Entry, error)
}

type DumpId int32

func (di *DumpId) GetDumpId() DumpId {
	atomic.AddInt32((*int32)(di), 1)
	return *di
}

type AttNum int
type Oid uint32
