package interfaces

import (
	"context"
)

type Restorer interface {
	Restore(ctx context.Context) error
	DebugInfo() string
	Meta() map[string]any
}
