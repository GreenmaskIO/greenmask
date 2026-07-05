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
	"bytes"
	"context"
	"io"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// --- Storager stub ---

type stubStorager struct {
	content []byte
	getErr  error
}

func (s *stubStorager) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func (s *stubStorager) GetCwd() string  { return "" }
func (s *stubStorager) Dirname() string { return "" }
func (s *stubStorager) ListDir(_ context.Context) ([]string, []core.Storager, error) {
	return nil, nil, nil
}
func (s *stubStorager) PutObject(_ context.Context, _ string, _ io.Reader) error { return nil }
func (s *stubStorager) Delete(_ context.Context, _ ...string) error              { return nil }
func (s *stubStorager) DeleteAll(_ context.Context, _ string) error              { return nil }
func (s *stubStorager) Exists(_ context.Context, _ string) (bool, error)         { return false, nil }
func (s *stubStorager) SubStorage(_ string, _ bool) core.Storager                { return s }
func (s *stubStorager) Stat(_ string) (*core.StorageObjectStat, error)           { return nil, nil }
func (s *stubStorager) Ping(_ context.Context) error                             { return nil }

// --- DatabaseSession stub ---

type stubSession struct {
	engineResFn func(ctx context.Context, fn func(context.Context, any) error) error
}

func (s *stubSession) Close(_ context.Context) error { return nil }
func (s *stubSession) RunWithOperationalDB(_ context.Context, _ func(context.Context, core.DB) error) error {
	return nil
}
func (s *stubSession) RunWithEngineResource(ctx context.Context, fn func(context.Context, any) error) error {
	if s.engineResFn != nil {
		return s.engineResFn(ctx, fn)
	}
	return nil
}

// --- ConnectionConfigurer stub ---

type stubConnConfigurer struct {
	config any
}

func (s stubConnConfigurer) ConnectionConfig() any { return s.config }

// --- RestoreRowReader stub ---

type stubReader struct {
	openErr     error
	rows        [][]byte
	rowIdx      int
	readErr     error // returned once all rows are consumed
	closeErr    error
	openCalled  int
	closeCalled int
}

func (r *stubReader) Open(_ context.Context, _ core.Storager) error {
	r.openCalled++
	return r.openErr
}

func (r *stubReader) ReadRow(_ context.Context) ([]byte, error) {
	if r.rowIdx < len(r.rows) {
		row := r.rows[r.rowIdx]
		r.rowIdx++
		return row, nil
	}
	if r.readErr != nil {
		return nil, r.readErr
	}
	return nil, core.ErrEndOfStream
}

func (r *stubReader) Close(_ context.Context) error {
	r.closeCalled++
	return r.closeErr
}

func (r *stubReader) DebugInfo() map[string]any { return nil }

// --- RestoreRowWriter stub ---

type stubWriter struct {
	openErr     error
	writeErr    error
	closeErr    error
	received    [][]byte
	openCalled  int
	closeCalled int
}

func (w *stubWriter) Open(_ context.Context, _ core.DatabaseSession, _ core.ConnectionConfigurer) error {
	w.openCalled++
	return w.openErr
}

func (w *stubWriter) WriteRow(_ context.Context, row []byte) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	dst := make([]byte, len(row))
	copy(dst, row)
	w.received = append(w.received, dst)
	return nil
}

func (w *stubWriter) Close(_ context.Context) error {
	w.closeCalled++
	return w.closeErr
}
