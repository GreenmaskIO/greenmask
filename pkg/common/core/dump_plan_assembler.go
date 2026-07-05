package core

import (
	"context"
)

type DumpPlanAssembler interface {
	Assemble(ctx context.Context, input DumpPlanInput) (DumpPlan, error)
}
