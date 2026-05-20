package interfaces

import "context"

type DumpPlanValidator interface {
	Validate(ctx context.Context, input DumpPlanValidationInput) (DumpValidationReport, error)
}
