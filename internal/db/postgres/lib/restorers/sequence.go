package restorers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

type SequenceRestorer struct {
	Entry *toc.Entry
}

func NewSequenceRestorer(entry *toc.Entry) *SequenceRestorer {
	return &SequenceRestorer{
		Entry: entry,
	}
}

func (td *SequenceRestorer) Execute(ctx context.Context, tx pgx.Tx) error {
	if td.Entry.Defn == nil {
		return fmt.Errorf("received nil pointer intead of sequence")
	}
	_, err := tx.Exec(ctx, *td.Entry.Defn)
	if err != nil {
		return fmt.Errorf("unable to apply sequence set val: %w", err)
	}
	return nil
}

func (td *SequenceRestorer) DebugInfo() string {
	return fmt.Sprintf("sequence %s.%s", *td.Entry.Namespace, *td.Entry.Tag)
}
