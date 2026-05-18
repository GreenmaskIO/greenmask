package table

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type Writer struct {
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Open(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (w *Writer) WriteRow(ctx context.Context, row [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (w *Writer) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (w *Writer) Stat() models.DumpedObjectStat {
	//TODO implement me
	panic("implement me")
}
