package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Dumper interface {
	Dump(ctx context.Context) (commonmodels.TaskStat, error)
	DebugInfo() string
	Meta() map[string]any
}
