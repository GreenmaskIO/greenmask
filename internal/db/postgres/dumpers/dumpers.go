package dumpers

import (
	"context"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storage"
)

type DumpTask interface {
	Execute(ctx context.Context, tx pgx.Tx, st storage.Storager) (toc.EntryProducer, error)
	DebugInfo() string
}
