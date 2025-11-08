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

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
	"github.com/greenmaskio/greenmask/v1/pkg/csv"
)

type TableDataWriter struct {
	st        storages.Storager
	usePgzip  bool
	fileName  string
	csvWriter *csv.Writer
	cw        utils.CountWriteCloser
	cr        utils.CountReadCloser
	eg        *errgroup.Group
	cancel    context.CancelFunc
	table     *commonmodels.Table
}

func NewTableDataWriter(
	table commonmodels.Table,
	st storages.Storager,
	usePgzip bool,
) *TableDataWriter {
	fileName := fmt.Sprintf("%s__%s.csv.gz", table.Schema, table.Name)
	return &TableDataWriter{
		st:       st,
		usePgzip: usePgzip,
		fileName: fileName,
		table:    &table,
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
	t.cw, t.cr = utils.NewGzipPipe(t.usePgzip)
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
	t.csvWriter.Flush()
	if err := t.cw.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	if err := t.eg.Wait(); err != nil {
		return fmt.Errorf("wait for stream: %w", err)
	}
	return nil
}

func (t *TableDataWriter) Stat() commonmodels.ObjectStat {
	if t.cw == nil {
		panic("writer is not opened")
	}
	if t.cr == nil {
		panic("reader is not opened")
	}
	return commonmodels.NewObjectStat(
		commonmodels.EngineMysql,
		commonmodels.ObjectKindTable,
		commonmodels.ObjectID(t.table.ID),
		t.table.FullTableName(),
		t.cw.GetCount(),
		t.cr.GetCount(),
		t.fileName,
	)
}
