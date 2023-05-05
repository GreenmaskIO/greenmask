package dumpers

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
)

type DumpTask interface {
	Execute(ctx context.Context, tx pgx.Tx, st storage.Storager) (*toc.Entry, error)
	DebugInfo() string
}
