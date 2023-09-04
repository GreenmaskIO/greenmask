package dumpers

import (
	"context"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/toc"
	"github.com/GreenmaskIO/greenmask/internal/storage"
)

type DumpTask interface {
	Execute(ctx context.Context, tx pgx.Tx, st storage.Storager) (toc.EntryProducer, error)
	DebugInfo() string
}
