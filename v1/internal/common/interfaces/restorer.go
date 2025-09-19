package interfaces

import (
	"context"
)

type Restorer interface {
	Init(ctx context.Context) error
	Close(ctx context.Context) error
	Restore(ctx context.Context) error
	DebugInfo() string
	Meta() map[string]any
}
