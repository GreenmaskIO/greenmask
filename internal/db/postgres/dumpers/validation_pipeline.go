package dumpers

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	dump "github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
)

type ValidationPipeline struct {
	*TransformationPipeline
	withOriginal bool
}

func NewValidationPipeline(ctx context.Context, eg *errgroup.Group, table *dump.Table, w io.Writer, withOriginal bool) (*ValidationPipeline, error) {
	tpp, err := NewTransformationPipeline(ctx, eg, table, w)
	if err != nil {
		return nil, err
	}
	return &ValidationPipeline{
		TransformationPipeline: tpp,
		withOriginal:           withOriginal,
	}, err
}

func (vp *ValidationPipeline) Dump(ctx context.Context, data []byte) (err error) {
	if vp.withOriginal {
		_, err = vp.w.Write(data)
		if err != nil {
			return NewDumpError(vp.table.Schema, vp.table.Name, vp.line, fmt.Errorf("error writing original dumped data: %w", err))
		}
	}

	return vp.TransformationPipeline.Dump(ctx, data)
}
