package interfaces

import "context"

type Pipeliner interface {
	DumpRow(ctx context.Context, row []byte) error
	Init(ctx context.Context) error
	Done(ctx context.Context) error
}
