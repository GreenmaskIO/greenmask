package core

import (
	"context"
)

type DumpContextDiffer interface {
	Diff(ctx context.Context, input DumpContextDiffInput) (DumpContextDiff, error)
}
