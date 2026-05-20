package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpProcessor interface {
	Run(ctx context.Context, plan models.DumpPlan) (models.Metadata, error)
}
