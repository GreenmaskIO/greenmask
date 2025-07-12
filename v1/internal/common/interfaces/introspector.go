package interfaces

import (
	"context"
	"database/sql"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Introspector interface {
	GetCommonTables() []commonmodels.Table
	Introspect(ctx context.Context, tx *sql.Tx) error
}
