package dump

import "github.com/greenmaskio/greenmask/internal/db/postgres/toc"

type Entry interface {
	toc.EntryProducer
	SetDumpId(dumpId int32)
}
