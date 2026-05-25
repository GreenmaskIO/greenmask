package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type ExplicitDumpContextBuilder interface {
	BuildDumpContext(ctx context.Context, in models.ExplicitDumpContextInput) (models.DumpContext, error)
}

type DerivedDumpContextBuilder interface {
	BuildDumpContext(ctx context.Context, in models.DerivedDumpContextInput) (models.DumpContext, error)
}
