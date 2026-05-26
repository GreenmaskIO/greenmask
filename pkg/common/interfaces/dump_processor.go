package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpProcessor interface {
	Run(ctx context.Context, session DumpSession, plan models.DumpPlan, opts ...models.DumpProcessorOption) (models.Metadata, error)
}
