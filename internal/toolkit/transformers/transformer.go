package transformers

import (
	"context"
)

type Transformer interface {
	Init(ctx context.Context) error
	Validate(ctx context.Context) (ValidationWarnings, error)
	Transform(ctx context.Context, r *Record) (*Record, error)
}
