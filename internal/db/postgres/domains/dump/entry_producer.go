package dump

import "github.com/GreenmaskIO/greenmask/internal/db/postgres/toc"

type Entry interface {
	toc.EntryProducer
	SetDumpId(dumpId int32)
}
