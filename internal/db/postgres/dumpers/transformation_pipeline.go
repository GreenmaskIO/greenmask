package dumpers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"io"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
)

type Pipeliner interface {
	Dump(ctx context.Context, data []byte) error
	CompleteDump() (err error)
}

type TransformationPipeline struct {
	table *dump.Table
	buf   *bytes.Buffer
	w     io.Writer
	line  int64
}

func NewTransformationPipeline(ctx context.Context, table *dump.Table, w io.Writer) (*TransformationPipeline, error) {
	buf := bytes.NewBuffer(nil)
	for _, t := range table.Transformers {
		if err := t.Init(ctx); err != nil {
			// TODO: Create new transformer error it would contain required context. Such as transformer name
			// 		 table name and so on
			log.Warn().Msg("IMPLEMENT ME: transformer error so it would contain required context. Such as transformer name table name and so on")
			return nil, fmt.Errorf("unable to initialize transformer")
		}
	}
	return &TransformationPipeline{
		table: table,
		buf:   buf,
		w:     w,
	}, nil
}

func (wt *TransformationPipeline) Dump(ctx context.Context, data []byte) (err error) {
	wt.line++
	_, err = wt.buf.Write(data)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
	}
	record := transformers.NewRecord(wt.table.Driver, pgcopy.NewRow(data[:len(data)-1]))
	for _, t := range wt.table.Transformers {
		record, err = t.Transform(ctx, record)
		if err != nil {
			return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
		}
	}
	rowDriver, err := record.Encode()
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error enocding to RowDriver: %w", err))
	}
	res, err := rowDriver.Encode()
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error RowDriver to []byte: %w", err))
	}
	res = append(res, '\n')
	_, err = wt.w.Write(res)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error writing dumped data: %w", err))
	}
	return nil
}

func (wt *TransformationPipeline) CompleteDump() (err error) {
	res := make([]byte, 4)
	res = append(res, pgcopy.DefaultCopyTerminationSeq...)
	res = append(res, '\n', '\n')
	_, err = wt.w.Write(res)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error end of dump symbols: %w", err))
	}
	return nil
}
