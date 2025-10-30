// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

const dumperTypeTableDumper = "table_dumper"

type TableDumper struct {
	ID               commonmodels.TaskID
	pipeline         commonininterfaces.Pipeliner
	dataStreamReader commonininterfaces.RowStreamReader
	dataStreamWriter commonininterfaces.RowStreamWriter
	record           commonininterfaces.Recorder
	lineNum          int64
	table            *commonmodels.Table
}

func NewTableDumper(
	id commonmodels.TaskID,
	dataStreamReader commonininterfaces.RowStreamReader,
	dataStreamWriter commonininterfaces.RowStreamWriter,
	record commonininterfaces.Recorder,
	pipeliner commonininterfaces.Pipeliner,
	table *commonmodels.Table,
) *TableDumper {
	return &TableDumper{
		ID:               id,
		dataStreamReader: dataStreamReader,
		dataStreamWriter: dataStreamWriter,
		record:           record,
		pipeline:         pipeliner,
		table:            table,
	}
}

func (t *TableDumper) Dump(ctx context.Context) (commonmodels.TaskStat, error) {
	startedAt := time.Now()
	// Initialize transformation pipeline.
	// It gets transformers ready to transform. For example if external transformer
	// is used then it starts its process.
	if err := t.pipeline.Init(ctx); err != nil {
		return commonmodels.TaskStat{}, commonmodels.NewDumpError(
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
		dumperTypeTableDumper,
		t.lineNum-1,
		commonmodels.EngineMysql,
		objectDefinition,
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
