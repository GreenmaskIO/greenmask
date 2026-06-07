package core

import (
	"context"
)

type DependencyGraphBuilder interface {
	BuildGraph(ctx context.Context, introspection IntrospectionResult) (DependencyGraphResult, error)
}
