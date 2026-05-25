package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpPlanValidator interface {
	Validate(ctx context.Context, input commonmodels.DumpPlanValidationInput) (commonmodels.DumpValidationReport, error)
}

type DumpContextValidator interface {
	Validate(ctx context.Context, input commonmodels.DumpContextValidatorInput) (commonmodels.DumpContextValidatorResult, error)
}
