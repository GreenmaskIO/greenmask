package dumpers

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"io"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
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

func (pdp *PlainDumpPipeline) CompleteDump(ctx context.Context) (err error) {
	res := make([]byte, 0, 4)
	res = append(res, pgcopy.DefaultCopyTerminationSeq...)
	res = append(res, '\n', '\n')
	_, err = pdp.w.Write(res)
	if err != nil {
		return NewDumpError(pdp.table.Schema, pdp.table.Name, pdp.line, fmt.Errorf("error end of dump symbols: %w", err))
	}
	return nil
}
