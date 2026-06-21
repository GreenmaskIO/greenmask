package core

import (
	"context"
)

type DumpProcessor interface {
	Run(ctx context.Context, input DumpRunInput) (Metadata, error)
}
