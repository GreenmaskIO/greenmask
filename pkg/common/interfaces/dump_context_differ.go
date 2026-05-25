package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpContextDiffer interface {
	Diff(ctx context.Context, input commonmodels.DumpContextDiffInput) (commonmodels.DumpContextDiff, error)
}
