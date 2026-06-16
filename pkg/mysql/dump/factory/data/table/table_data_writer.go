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

package table

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

const (
	ExtensionSql  = "sql"
	ExtensionGzip = "gz"
)

var errStorageWasNotSet = fmt.Errorf("destination storage is not set: call Init before Open")

type RowWriter interface {
	Write(row [][]byte) error
	Flush() error
}

type Option func(*TableDataWriter)

type TableDataWriter struct {
	st        core.Storager
	fileName  string
	rowWriter RowWriter
	cw        utils.CountWriteCloser
	cr        utils.CountReadCloser
	eg        *errgroup.Group
	cancel    context.CancelFunc
	table     *core.Table
	enabled   bool
	pgzip     bool
	hexBlob   bool
}

func NewTableDataWriter(
	table core.Table,
	opts ...Option,
) *TableDataWriter {
	res := &TableDataWriter{
		table: &table,
	}

	for _, opt := range opts {
		opt(res)
	}

	ext := ExtensionSql
	if res.enabled {
		ext += "." + ExtensionGzip
	}
	res.fileName = fmt.Sprintf("%s__%s.%s", table.Schema, table.Name, ext)
	return res
}

func WithCompression(enabled bool) Option {
	return func(t *TableDataWriter) {
		t.enabled = enabled
	}
}

func WithPgzip(enabled bool) Option {
	return func(t *TableDataWriter) {
		t.pgzip = enabled
	}
}

func WithHexBlob(enabled bool) Option {
	return func(t *TableDataWriter) {
		t.hexBlob = enabled
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

// Open binds the destination storage and opens the stream.
func (t *TableDataWriter) Open(ctx context.Context, st core.Storager) error {
	if st == nil {
		return errStorageWasNotSet
	}
	t.st = st
	if t.enabled {
		t.cw, t.cr = utils.NewGzipPipe(t.pgzip)
	} else {
		t.cw, t.cr = utils.NewPlainPipe()
	}

	t.rowWriter = NewInsertWriter(*t.table, t.cw, t.hexBlob)
	ctx, t.cancel = context.WithCancel(ctx)
	t.eg, ctx = errgroup.WithContext(ctx)
	t.eg.Go(t.steam(ctx))
	return nil
}

func (t *TableDataWriter) WriteRow(_ context.Context, row [][]byte) error {
	if err := t.rowWriter.Write(row); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	return nil
}

func (t *TableDataWriter) Close(_ context.Context) error {
	if err := t.rowWriter.Flush(); err != nil {
		return fmt.Errorf("flush row writer: %w", err)
	}
	if err := t.cw.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}
	if err := t.eg.Wait(); err != nil {
		return fmt.Errorf("wait for stream: %w", err)
	}
	return nil
}

func (t *TableDataWriter) Stat() core.DumpedObjectStat {
	if t.cw == nil {
		panic("writer is not opened")
	}
	if t.cr == nil {
		panic("reader is not opened")
	}
	compression := core.CompressionNone
	if t.enabled {
		compression = core.CompressionGzip
		if t.pgzip {
			compression = core.CompressionPgzip
		}
	}

	return core.NewObjectStat(
		core.DBMSEngineMySQL,
		core.ObjectKindTable,
		core.ObjectID(t.table.ID),
		t.table.FullTableName(),
		t.cw.GetCount(),
		t.cr.GetCount(),
		t.fileName,
		compression,
		core.DumpFormatInsert,
	)
}
