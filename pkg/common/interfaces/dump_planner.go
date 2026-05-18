package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpPlanner interface {
	BuildPlan(ctx context.Context, result commonmodels.IntrospectionResult) (commonmodels.DumpPlan, error)
}
