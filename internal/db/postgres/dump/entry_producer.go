package dump

import "github.com/greenmaskio/greenmask/internal/db/postgres/toc"

type Entry interface {
	Entry() (*toc.Entry, error)
	SetDumpId(dumpId int32)
}
