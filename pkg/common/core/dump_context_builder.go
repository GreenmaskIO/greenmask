package core

import (
	"context"
)

type ExplicitDumpContextBuilder interface {
	BuildDumpContext(ctx context.Context, in ExplicitDumpContextInput) (DumpContext, error)
}

type DerivedDumpContextBuilder interface {
	BuildDumpContext(ctx context.Context, in DerivedDumpContextInput) (DumpContext, error)
}
