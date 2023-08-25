package base

import (
	"context"

	utils "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

type Transformer interface {
	Init(ctx context.Context) error
	Validate(ctx context.Context) error
	Transform(ctx context.Context, r *utils.Record) (*utils.Record, error)
}
