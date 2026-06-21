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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

var _ core.RestoreRowReader = (*TableRestoreReader)(nil)

// TableRestoreReader is the restore-side symmetric counterpart of TableDataWriter.
// It opens a dump file from storage, decompresses it if needed, and returns each
// non-empty line (values tuple) one by one via ReadRow.
type TableRestoreReader struct {
	filename    string
	compression core.Compression
	rc          io.ReadCloser
	scanner     *bufio.Scanner
}

func NewTableRestoreReader(filename string, compression core.Compression) *TableRestoreReader {
	return &TableRestoreReader{
		filename:    filename,
		compression: compression,
	}
}

func (r *TableRestoreReader) Open(ctx context.Context, st core.Storager) error {
	f, err := st.GetObject(ctx, r.filename)
	if err != nil {
		return fmt.Errorf("open table data file %q: %w", r.filename, err)
	}

	var rc io.ReadCloser = f
	if r.compression.IsEnabled() {
		rc, err = utils.NewGzipReader(f, r.compression.IsPgzip())
		if err != nil {
			return fmt.Errorf("create gzip reader for %q: %w", r.filename, err)
		}
	}
	r.rc = rc
	r.scanner = bufio.NewScanner(r.rc)
	return nil
}

// ReadRow returns the next non-empty row (values tuple bytes) from the dump
// file. Returns core.ErrEndOfStream when the file is exhausted.
func (r *TableRestoreReader) ReadRow(_ context.Context) ([]byte, error) {
	for r.scanner.Scan() {
		line := r.scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		row := make([]byte, len(line))
		copy(row, line)
		return row, nil
	}
	if err := r.scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan table data file: %w", err)
	}
	return nil, core.ErrEndOfStream
}

func (r *TableRestoreReader) Close(ctx context.Context) error {
	if r.rc == nil {
		return nil
	}
	if err := r.rc.Close(); err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("file", r.filename).Msg("close table restore reader")
	}
	return nil
}

func (r *TableRestoreReader) DebugInfo() map[string]any {
	return map[string]any{
		"filename":    r.filename,
		"compression": r.compression,
	}
}
