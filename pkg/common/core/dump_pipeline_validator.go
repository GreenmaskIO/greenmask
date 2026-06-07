package core

import (
	"context"
)

// DumpPlanValidator validates the final executable dump plan.
// Warnings and errors are collected via validationcollector from context.
// A fatal validation error surfaces as ErrFatalValidationError.
type DumpPlanValidator interface {
	Validate(ctx context.Context, input DumpPlanValidationInput) error
}

// DumpContextValidator validates semantic correctness of the dump context.
// Warnings and errors are collected via validationcollector from context.
// A fatal validation error surfaces as ErrFatalValidationError.
type DumpContextValidator interface {
	Validate(ctx context.Context, input DumpContextValidatorInput) error
}
