package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type SubsetBuilder interface {
	BuildSubset(ctx context.Context, in commonmodels.SubsetBuilderInput) (commonmodels.SubsetResult, error)
}
