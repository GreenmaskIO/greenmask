package dump

import "github.com/wwoytenko/greenfuscator/internal/db/postgres/toc"

type Entry interface {
	toc.EntryProducer
	SetDumpId(dumpId int32)
}
