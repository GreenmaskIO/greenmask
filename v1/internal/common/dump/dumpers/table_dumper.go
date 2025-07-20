package dumpers

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

const dumperTypeTableDumper = "TableDumper"

type TableDumper struct {
	pipeline         commonininterfaces.Pipeliner
	dataStreamReader commonininterfaces.RowStreamReader
	dataStreamWriter commonininterfaces.RowStreamWriter
	record           commonininterfaces.Recorder
	lineNum          int64
}

func NewTableDumper(
	dataStreamReader commonininterfaces.RowStreamReader,
	dataStreamWriter commonininterfaces.RowStreamWriter,
	record commonininterfaces.Recorder,
	pipeliner commonininterfaces.Pipeliner,
) *TableDumper {
	return &TableDumper{
		dataStreamReader: dataStreamReader,
		dataStreamWriter: dataStreamWriter,
		record:           record,
		pipeline:         pipeliner,
	}
}

func (t *TableDumper) Dump(ctx context.Context) (commonmodels.DumpStat, error) {
	startedAt := time.Now()
	// Initialize transformation pipeline.
	// It gets transformers ready to transform. For example if external transformer
	// is used then it starts its process.
	if err := t.pipeline.Init(ctx); err != nil {
		return commonmodels.DumpStat{}, commonmodels.NewDumpError(
			t.lineNum, fmt.Errorf("init transformation pipeline: %w", err),
		)
	}

	defer func() {
		// Terminate transformers that were started.
		if err := t.pipeline.Done(ctx); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Msg("error closing transformation pipeline")
		}
	}()

	// Stream records and transform them one by one.
	if err := t.streamRecords(ctx); err != nil {
		return commonmodels.DumpStat{}, commonmodels.NewDumpError(
			t.lineNum, fmt.Errorf("stream data: %w", err),
		)
	}

	return commonmodels.NewDumpStat(
		t.dataStreamWriter.Stat(),
		time.Since(startedAt),
		dumperTypeTableDumper,
	), nil
}

// dumper - dumps the data from the table and transform it if needed
func (t *TableDumper) dataDumper(ctx context.Context) func() error {
	return func() error {
		// Initialize transformation pipeline.
		// It gets transformers ready to transform. For example if external transformer
		// is used then it starts its process.
		if err := t.pipeline.Init(ctx); err != nil {
			return commonmodels.NewDumpError(t.lineNum, fmt.Errorf("init transformation pipeline: %w", err))
		}

		defer func() {
			// Terminate transformers that were started.
			if err := t.pipeline.Done(ctx); err != nil {
				log.Ctx(ctx).
					Warn().
					Err(err).
					Msg("error closing transformation pipeline")
			}
		}()

		// Stream records and transform them one by one.
		if err := t.streamRecords(ctx); err != nil {
			return commonmodels.NewDumpError(t.lineNum, fmt.Errorf("stream data: %w", err))
		}
		return nil
	}
}

func (t *TableDumper) streamRecords(ctx context.Context) error {
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
		if err := t.record.SetRow(row); err != nil {
			return fmt.Errorf("set raw record data: %w", err)
		}
		if err := t.pipeline.Transform(ctx, t.record); err != nil {
			return fmt.Errorf("run transform: %w", err)
		}
		if err := t.dataStreamWriter.WriteRow(ctx, t.record.GetRow()); err != nil {
			return fmt.Errorf("write transformed raw data: %w", err)
		}
	}
}

func (t *TableDumper) Meta() map[string]any {
	meta := t.dataStreamReader.DebugInfo()
	uniqueDumpTaskID := getUniqueDumpTaskID(dumperTypeTableDumper, meta)
	meta = maps.Clone(meta)
	meta[commonmodels.MetaKeyUniqueDumpTaskID] = uniqueDumpTaskID
	return meta
}

func (t *TableDumper) DebugInfo() string {
	return getUniqueDumpTaskID(dumperTypeTableDumper, t.dataStreamReader.DebugInfo())
}
