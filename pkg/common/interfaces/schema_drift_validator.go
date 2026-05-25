package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type SchemaDriftValidator interface {
	Compare(ctx context.Context, input commonmodels.SchemaDriftValidatorInput) commonmodels.SchemaDriftResult
}
