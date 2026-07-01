package core

import (
	"context"
)

type DumpContextSnapshotBuilder interface {
	Build(
		ctx context.Context,
		input DumpContext,
	) (DumpContextSnapshot, error)
}
