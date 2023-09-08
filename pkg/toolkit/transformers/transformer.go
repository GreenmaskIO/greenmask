package transformers

import (
	"context"
)

type Transformer interface {
	Init(ctx context.Context) error
	Transform(ctx context.Context, r *Record) (*Record, error)
}
