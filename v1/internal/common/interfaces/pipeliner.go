package interfaces

import "context"

type Pipeliner interface {
	Transform(ctx context.Context, record Recorder) error
	Init(ctx context.Context) error
	Done(ctx context.Context) error
}
