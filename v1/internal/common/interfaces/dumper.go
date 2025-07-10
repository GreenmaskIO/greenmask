package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Dumper interface {
	Dump(ctx context.Context) (commonmodels.DumpStat, error)
	DebugInfo() string
}
