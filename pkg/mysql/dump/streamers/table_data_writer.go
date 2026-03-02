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

package streamers

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	utils2 "github.com/greenmaskio/greenmask/pkg/common/utils"

	"github.com/greenmaskio/greenmask/pkg/csv"
)

type CompressionSettings struct {
	Enabled bool
	Pgzip   bool
}

type TableDataWriter struct {
	st           interfaces.Storager
	fileName     string
	csvWriter    *csv.Writer
	cw           utils2.CountWriteCloser
	cr           utils2.CountReadCloser
	eg           *errgroup.Group
	cancel       context.CancelFunc
	table        *models.Table
	compSettings CompressionSettings
}

func NewTableDataWriter(
	table models.Table,
	st interfaces.Storager,
	compSettings CompressionSettings,
) *TableDataWriter {
	ext := "csv"
	if compSettings.Enabled {
		ext = "csv.gz"
	}
	fileName := fmt.Sprintf("%s__%s.%s", table.Schema, table.Name, ext)
	return &TableDataWriter{
		st:           st,
		fileName:     fileName,
		table:        &table,
		compSettings: compSettings,
	}
}

func (t *TableDataWriter) steam(ctx context.Context) func() error {
	return func() error {
		if err := t.st.PutObject(ctx, t.fileName, t.cr); err != nil {
			return fmt.Errorf("put object: %w", err)
		}
		return nil
	}
}

func (t *TableDataWriter) Open(ctx context.Context) error {
	if t.compSettings.Enabled {
		t.cw, t.cr = utils2.NewGzipPipe(t.compSettings.Pgzip)
	} else {
		t.cw, t.cr = utils2.NewPlainPipe()
	}

	t.csvWriter = csv.NewWriter(t.cw)
	ctx, t.cancel = context.WithCancel(ctx)
	t.eg, ctx = errgroup.WithContext(ctx)
	t.eg.Go(t.steam(ctx))
	return nil
}

func (t *TableDataWriter) WriteRow(_ context.Context, row [][]byte) error {
	if err := t.csvWriter.Write(row); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}
	return nil
}

func (t *TableDataWriter) Close(_ context.Context) error {
	if err := t.csvWriter.Flush(); err != nil {
		return fmt.Errorf("flush csv writer: %w", err)
	}
	if err := t.cw.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	if err := t.eg.Wait(); err != nil {
		return fmt.Errorf("wait for stream: %w", err)
	}
	return nil
}

func (t *TableDataWriter) Stat() models.ObjectStat {
	if t.cw == nil {
		panic("writer is not opened")
	}
	if t.cr == nil {
		panic("reader is not opened")
	}
	compression := models.CompressionNone
	if t.compSettings.Enabled {
		compression = models.CompressionGzip
		if t.compSettings.Pgzip {
			compression = models.CompressionPgzip
		}
	}

	return models.NewObjectStat(
		models.EngineMysql,
		models.ObjectKindTable,
		models.ObjectID(t.table.ID),
		t.table.FullTableName(),
		t.cw.GetCount(),
		t.cr.GetCount(),
		t.fileName,
		compression,
	)
}
