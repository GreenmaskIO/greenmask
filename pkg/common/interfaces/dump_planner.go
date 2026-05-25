package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type RestorationContextBuilder interface {
	Build(ctx context.Context, input commonmodels.RestorationContextInput) (commonmodels.RestorationContext, error)
}
