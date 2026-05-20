package interfaces

import commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"

type DependencyGraphBuilder interface {
	BuildGraph(introspection commonmodels.IntrospectionResult) (commonmodels.DependencyGraphResult, error)
}
