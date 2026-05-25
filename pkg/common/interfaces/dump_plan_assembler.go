package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpPlanAssembler interface {
	Assemble(ctx context.Context, input commonmodels.DumpPlanInput) (commonmodels.DumpPlan, error)
}
