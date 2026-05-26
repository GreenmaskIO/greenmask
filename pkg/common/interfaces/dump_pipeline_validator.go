package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

// DumpPlanValidator validates the final executable dump plan.
// Warnings and errors are collected via validationcollector from context.
// A fatal validation error surfaces as models.ErrFatalValidationError.
type DumpPlanValidator interface {
	Validate(ctx context.Context, input commonmodels.DumpPlanValidationInput) error
}

// DumpContextValidator validates semantic correctness of the dump context.
// Warnings and errors are collected via validationcollector from context.
// A fatal validation error surfaces as models.ErrFatalValidationError.
type DumpContextValidator interface {
	Validate(ctx context.Context, input commonmodels.DumpContextValidatorInput) error
}
