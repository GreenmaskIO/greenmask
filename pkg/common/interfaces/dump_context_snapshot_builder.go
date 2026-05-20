package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpContextSnapshotBuilder interface {
	Build(
		ctx context.Context,
		input models.DumpContext,
	) (models.DumpContextSnapshot, error)
}
