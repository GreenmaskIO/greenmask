package toolkit

import (
	"context"
)

type NewTransformerFunc func(ctx context.Context, driver *Driver, parameters map[string]*Parameter) (
	Transformer, ValidationWarnings, error,
)

type Transformer interface {
	Validate(ctx context.Context) (ValidationWarnings, error)
	Transform(ctx context.Context, r *Record) (*Record, error)
}
