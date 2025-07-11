package dumpers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

const dumperTypeTableRawDumper = "table_raw_dumper"

type TableRawDumper struct {
	dataStreamReader commonininterfaces.RowStreamReader
	dataStreamWriter commonininterfaces.RowStreamWriter
	lineNum          int64
}

func NewTableRawDumper(
	dataStreamReader commonininterfaces.RowStreamReader,
	dataStreamWriter commonininterfaces.RowStreamWriter,
) *TableRawDumper {
	return &TableRawDumper{
		dataStreamReader: dataStreamReader,
		dataStreamWriter: dataStreamWriter,
	}
}

func (t *TableRawDumper) Dump(ctx context.Context) (commonmodels.DumpStat, error) {
	startedAt := time.Now()

	// Stream records and transform them one by one.
	if err := t.streamRecords(ctx); err != nil {
		return commonmodels.DumpStat{}, commonmodels.NewDumpError(
			t.lineNum, fmt.Errorf("stream data: %w", err),
		)
	}

	return commonmodels.NewDumpStat(
		t.dataStreamWriter.Stat(),
		time.Since(startedAt),
		dumperTypeTableRawDumper,
	), nil
}

func (t *TableRawDumper) streamRecords(ctx context.Context) error {
	// Open stream reader - the one that reads data from table in DBMS.
	if err := t.dataStreamReader.Open(ctx); err != nil {
		return fmt.Errorf("open data streamer: %w", err)
	}
	defer func() {
		// Close stream reader.
		if err := t.dataStreamReader.Close(ctx); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Msg("error closing data streamer reader")
		}
	}()
	// Open stream writer - the one that writes transformed data
	// directly to the storage.
	if err := t.dataStreamWriter.Open(ctx); err != nil {
		return fmt.Errorf("open data streamer: %w", err)
	}
	defer func() {
		// Close stream writer. The one that stores data
		// into the storage.
		if err := t.dataStreamWriter.Close(ctx); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Msg("error closing data stream writer")
		}
	}()
	for {
		t.lineNum++
		row, err := t.dataStreamReader.ReadRow(ctx)
		if err != nil {
			if errors.Is(err, commonmodels.ErrEndOfStream) {
				return nil
			}
			return fmt.Errorf("read row from stream: %w", err)
		}
		if err := t.dataStreamWriter.WriteRow(ctx, row); err != nil {
			return fmt.Errorf("write transformed raw data: %w", err)
		}
	}
}

func (t *TableRawDumper) DebugInfo() map[string]any {
	return t.dataStreamReader.DebugInfo()
}
