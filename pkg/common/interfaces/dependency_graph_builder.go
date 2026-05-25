package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type DependencyGraphBuilder interface {
	BuildGraph(ctx context.Context, introspection commonmodels.IntrospectionResult) (commonmodels.DependencyGraphResult, error)
}
