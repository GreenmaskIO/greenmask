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

// Package table implements the MySQL table data restore factory for V2.
package table

import (
	"context"
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.ObjectRestorer = (*TableRestorer)(nil)

// TableRestorer is a core.ObjectRestorer that pumps rows from a RestoreRowReader
// (dump file in storage) to a RestoreRowWriter (target DB). It is the symmetric
// counterpart of TableDumper/TableRawDumper on the dump side.
type TableRestorer struct {
	spec   core.ObjectRestoreSpec
	table  *core.Table
	reader core.RestoreRowReader
	writer core.RestoreRowWriter
}

func NewTableRestorer(
	spec core.ObjectRestoreSpec,
	table *core.Table,
	reader core.RestoreRowReader,
	writer core.RestoreRowWriter,
) *TableRestorer {
	return &TableRestorer{
		spec:   spec,
		table:  table,
		reader: reader,
		writer: writer,
	}
}

func (r *TableRestorer) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", r.table.Schema, r.table.Name)
}

func (r *TableRestorer) Meta() map[string]any {
	return map[string]any{
		core.MetaKeyTableSchema: r.table.Schema,
		core.MetaKeyTableName:   r.table.Name,
	}
}

// Restore opens the reader and writer, then pumps each row from the dump file
// to the target DB. The writer holds a single DB transaction for the full table.
func (r *TableRestorer) Restore(
	ctx context.Context,
	session core.DatabaseSession,
	conn core.ConnectionConfigurer,
	st core.Storager,
) (retErr error) {
	if err := r.reader.Open(ctx, st); err != nil {
		return fmt.Errorf("open table restore reader: %w", err)
	}
	defer func() {
		if err := r.reader.Close(ctx); err != nil && retErr == nil {
			retErr = fmt.Errorf("close table restore reader: %w", err)
		}
	}()

	if err := r.writer.Open(ctx, session, conn); err != nil {
		return fmt.Errorf("open table restore writer: %w", err)
	}
	defer func() {
		if err := r.writer.Close(ctx); err != nil && retErr == nil {
			retErr = fmt.Errorf("close table restore writer: %w", err)
		}
	}()

	for {
		row, err := r.reader.ReadRow(ctx)
		if errors.Is(err, core.ErrEndOfStream) {
			break
		}
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}
		if err := r.writer.WriteRow(ctx, row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	return nil
}
