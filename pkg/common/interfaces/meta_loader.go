package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpMetadataLoader interface {
	LoadPrevious(ctx context.Context, input models.PreviousMetadataLoadInput) (*models.Metadata, error)
}
