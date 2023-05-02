package dumpers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
)

type SequenceDumper struct {
	sequence *domains.Sequence
}

func NewSequenceDumper(sequence domains.Sequence) *SequenceDumper {
	return &SequenceDumper{
		sequence: &sequence,
	}
}

func (sd *SequenceDumper) Execute(ctx context.Context, tx pgx.Tx, st storage.Storager) (*toc.Entry, error) {
	return sd.sequence.GetTocEntry()
}

func (sd *SequenceDumper) DebugInfo() string {
	return fmt.Sprintf("sequence %s.%s", sd.sequence.Schema, sd.sequence.Name)
}
