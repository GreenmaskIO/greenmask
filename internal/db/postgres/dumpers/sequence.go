package dumpers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/internal/db/postgres/domains/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
)

type SequenceDumper struct {
	sequence *dump.Sequence
}

func NewSequenceDumper(sequence *dump.Sequence) *SequenceDumper {
	return &SequenceDumper{
		sequence: sequence,
	}
}

func (sd *SequenceDumper) Execute(ctx context.Context, tx pgx.Tx, st storages.Storager) (toc.EntryProducer, error) {
	return sd.sequence, nil
}

func (sd *SequenceDumper) DebugInfo() string {
	return fmt.Sprintf("sequence %s.%s", sd.sequence.Schema, sd.sequence.Name)
}
