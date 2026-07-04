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
