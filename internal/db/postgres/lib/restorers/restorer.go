package restorers

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type RestoreTask interface {
	Execute(ctx context.Context, tx pgx.Tx) error
	DebugInfo() string
}
