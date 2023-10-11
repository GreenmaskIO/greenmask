package dumpers

import (
	"context"
)

type Pipeliner interface {
	Dump(ctx context.Context, data []byte) error
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	CompleteDump() error
}
