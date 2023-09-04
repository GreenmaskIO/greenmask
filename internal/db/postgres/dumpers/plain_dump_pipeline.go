package dumpers

import (
	"context"
	"io"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/dump"
)

type PlainDumpPipeline struct {
	w     io.Writer
	line  int64
	table *dump.Table
}

func NewPlainDumpPipeline(table *dump.Table, w io.Writer) *PlainDumpPipeline {
	return &PlainDumpPipeline{
		table: table,
		w:     w,
	}
}

func (pdp *PlainDumpPipeline) Dump(ctx context.Context, data []byte) (err error) {
	pdp.line++
	if _, err := pdp.w.Write(data); err != nil {
		return NewDumpError(pdp.table.Schema, pdp.table.Name, pdp.line, err)
	}
	return nil
}
