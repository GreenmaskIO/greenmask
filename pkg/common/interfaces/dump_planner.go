package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type RestorationContextBuilder interface {
	BuildRestorationContext(ctx context.Context, result commonmodels.DumpContext) (commonmodels.RestorationContext, error)
}
