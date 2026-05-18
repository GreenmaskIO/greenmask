package table

import (
	"context"
)

type Reader struct {
}

func NewReader() *Reader {
	return &Reader{}
}

func (r *Reader) Open(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (r *Reader) ReadRow(ctx context.Context) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (r *Reader) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (r *Reader) DebugInfo() map[string]any {
	//TODO implement me
	panic("implement me")
}
