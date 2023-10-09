package utils

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type Transformer interface {
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error)
	GetAffectedColumns() map[int]string
}
