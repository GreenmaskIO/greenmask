package postgres

import (
	"context"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgrestore"
	"github.com/wwoytenko/greenfuscator/internal/storage"
)

type Restore struct {
	binPath string
	st      storage.Storager
}

func NewRestore(binPath string, st storage.Storager) *Restore {
	return &Restore{
		binPath: binPath,
		st:      st,
	}
}

func (r *Restore) RunRestore(ctx context.Context, opt *pgrestore.Options, dumpId string) error {
	return nil
}
