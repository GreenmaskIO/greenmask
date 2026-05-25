package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type ConfigEditor interface {
	EditConfig(ctx context.Context, input commonmodels.ConfigEditInput) []commonmodels.TableConfig
}
