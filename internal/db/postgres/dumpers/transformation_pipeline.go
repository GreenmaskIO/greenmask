package dumpers

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	toolkitUtils "github.com/wwoytenko/greenfuscator/internal/toolkit/utils"
)

type Pipeliner interface {
	Dump(ctx context.Context, data []byte) error
}

type TransformationPipeline struct {
	table        *dump.Table
	buf          *bytes.Buffer
	streamDriver *toolkitUtils.StreamDriver
	line         int64
}

func NewTransformationPipeline(ctx context.Context, buf *bytes.Buffer, table *dump.Table, w io.Writer) (*TransformationPipeline, error) {
	for _, t := range table.Transformers {
		if err := t.Init(ctx); err != nil {
			// TODO: Create new transformer error it would contain required context. Such as transformer name
			// 		 table name and so on
			log.Warn().Msg("IMPLEMENT ME: transformer error so it would contain required context. Such as transformer name table name and so on")
			return nil, fmt.Errorf("unable to initialize transformer")
		}
	}
	return &TransformationPipeline{
		table:        table,
		buf:          buf,
		streamDriver: toolkitUtils.NewStreamDriver(buf, w, table.Driver),
	}, nil
}

func (wt *TransformationPipeline) Dump(ctx context.Context, data []byte) (err error) {
	wt.line++
	_, err = wt.buf.Write(data)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
	}
	record, err := wt.streamDriver.Read()
	for _, t := range wt.table.Transformers {
		record, err = t.Transform(ctx, record)
		if err != nil {
			return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
		}
	}
	if err = wt.streamDriver.Write(record); err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
	}
	return nil
}
