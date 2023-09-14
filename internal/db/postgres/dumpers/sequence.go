package dumpers

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
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

func (sd *SequenceDumper) Execute(ctx context.Context, tx pgx.Tx, st storages.Storager) (dump.Entry, error) {
	return sd.sequence, nil
}

func (sd *SequenceDumper) DebugInfo() string {
	return fmt.Sprintf("sequence %s.%s", sd.sequence.Schema, sd.sequence.Name)
}
