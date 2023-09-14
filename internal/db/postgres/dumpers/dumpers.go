package dumpers

import (
	"context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/internal/storages"
)

type DumpTask interface {
	Execute(ctx context.Context, tx pgx.Tx, st storages.Storager) (dump.Entry, error)
	DebugInfo() string
}
