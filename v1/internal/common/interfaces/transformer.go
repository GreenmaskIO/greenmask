package interfaces

import (
	"context"
)

type Transformer interface {
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	Transform(ctx context.Context, r Recorder) error
	GetAffectedColumns() map[int]string
}
