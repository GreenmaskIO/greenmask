package dumpers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

const dumperTypeTableRawDumper = "table_raw_dumper"

type TableRawDumper struct {
	ID               commonmodels.TaskID
	dataStreamReader commonininterfaces.RowStreamReader
	dataStreamWriter commonininterfaces.RowStreamWriter
	lineNum          int64
	table            *commonmodels.Table
}

func NewTableRawDumper(
	id commonmodels.TaskID,
	dataStreamReader commonininterfaces.RowStreamReader,
	dataStreamWriter commonininterfaces.RowStreamWriter,
	table *commonmodels.Table,
) *TableRawDumper {
	return &TableRawDumper{
		ID:               id,
		dataStreamReader: dataStreamReader,
		dataStreamWriter: dataStreamWriter,
		lineNum:          0,
		table:            table,
	}
}

func (t *TableRawDumper) Dump(ctx context.Context) (commonmodels.TaskStat, error) {
	startedAt := time.Now()

	// Stream records and transform them one by one.
	if err := t.stream(ctx); err != nil {
		return commonmodels.TaskStat{}, commonmodels.NewDumpError(
			t.lineNum, fmt.Errorf("stream data: %w", err),
		)
	}

	objectDefinition, err := json.Marshal(*t.table)
	if err != nil {
		return commonmodels.TaskStat{}, fmt.Errorf("marshalling table definition: %w", err)
	}

	return commonmodels.NewDumpStat(
		t.ID,
		t.dataStreamWriter.Stat(),
		time.Since(startedAt),
		dumperTypeTableRawDumper,
		t.lineNum-1,
		commonmodels.EngineMysql,
		objectDefinition,
	), nil
}

func (t *TableRawDumper) streamRecords(ctx context.Context) error {
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

func (t *TableRawDumper) stream(ctx context.Context) error {
	// Open stream reader - the one that reads data from table in DBMS.
	if err := t.dataStreamReader.Open(ctx); err != nil {
		return fmt.Errorf("open data streamer: %w", err)
	}
	// Open stream writer - the one that writes transformed data
	// directly to the storage.
	if err := t.dataStreamWriter.Open(ctx); err != nil {
		return fmt.Errorf("open data streamer: %w", err)
	}

	if err := t.streamRecords(ctx); err != nil {
		log.Ctx(ctx).
			Warn().
			Err(err).
			Msg("error streaming records")
		lastErr := err

		// Close stream writer. The one that stores data into the storage.
		if err := t.dataStreamWriter.Close(ctx); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Msg("error closing data stream writer")
		}
		// Close stream reader - the one that gets data from table.
		if err := t.dataStreamReader.Close(ctx); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Msg("error closing data streamer reader")
		}
		return fmt.Errorf("stream records records: %w", lastErr)
	}

	var lastErr error
	// Close stream writer. The one that stores data into the storage.
	if err := t.dataStreamWriter.Close(ctx); err != nil {
		lastErr = fmt.Errorf("close data stream writer: %w", err)
		log.Ctx(ctx).
			Warn().
			Err(err).
			Msg("error closing data stream writer")
	}
	// Close stream reader - the one that gets data from table.
	if err := t.dataStreamReader.Close(ctx); err != nil {
		lastErr = fmt.Errorf("close data streame reader: %w", err)
		log.Ctx(ctx).
			Warn().
			Err(err).
			Msg("error closing data streamer reader")
	}
	if lastErr != nil {
		return fmt.Errorf("close data stream writer or reader: %w", lastErr)
	}
	return nil
}

func (t *TableRawDumper) Meta() map[string]any {
	meta := t.dataStreamReader.DebugInfo()
	uniqueDumpTaskID := getUniqueDumpTaskID(dumperTypeTableDumper, meta)
	meta = maps.Clone(meta)
	meta[commonmodels.MetaKeyUniqueDumpTaskID] = uniqueDumpTaskID
	return meta
}

func (t *TableRawDumper) DebugInfo() string {
	return getUniqueDumpTaskID(dumperTypeTableDumper, t.dataStreamReader.DebugInfo())
}
