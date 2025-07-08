package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

type Dumper interface {
	Dump(ctx context.Context, st storages.Storager) (commonmodels.DumpStat, error)
	DebugInfo() string
}
