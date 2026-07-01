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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func TestTableRestorer_DebugInfo(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{"simple", "mydb", "orders", "table mydb.orders"},
		{"another", "public", "users", "table public.users"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := testTable(tc.schema, tc.table, "id")
			r := NewTableRestorer(core.ObjectRestoreSpec{}, table, &stubReader{}, &stubWriter{})
			assert.Equal(t, tc.want, r.DebugInfo())
		})
	}
}

func TestTableRestorer_Meta(t *testing.T) {
	table := testTable("mydb", "orders", "id", "amount")
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, &stubReader{}, &stubWriter{})
	meta := r.Meta()
	assert.Equal(t, "mydb", meta[core.MetaKeyTableSchema])
	assert.Equal(t, "orders", meta[core.MetaKeyTableName])
	assert.Len(t, meta, 2)
}

func TestTableRestorer_Restore_HappyPath(t *testing.T) {
	table := testTable("db", "users", "id", "name")
	rows := [][]byte{[]byte("(1,'alice')"), []byte("(2,'bob')"), []byte("(3,'carol')")}
	reader := &stubReader{rows: rows}
	writer := &stubWriter{}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.NoError(t, err)

	assert.Equal(t, 1, reader.openCalled)
	assert.Equal(t, 1, reader.closeCalled)
	assert.Equal(t, 1, writer.openCalled)
	assert.Equal(t, 1, writer.closeCalled)
	assert.Equal(t, rows, writer.received)
}

func TestTableRestorer_Restore_EmptyTable(t *testing.T) {
	table := testTable("db", "empty_table", "id")
	reader := &stubReader{}
	writer := &stubWriter{}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.NoError(t, err)
	assert.Empty(t, writer.received)
}

func TestTableRestorer_Restore_ReaderOpenError(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{openErr: errors.New("storage unreachable")}
	writer := &stubWriter{}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "open table restore reader")
	// reader.Open failed so the defer was never registered → Close must not be called
	assert.Equal(t, 0, reader.closeCalled)
	// writer.Open must never be reached
	assert.Equal(t, 0, writer.openCalled)
}

func TestTableRestorer_Restore_WriterOpenError(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{}
	writer := &stubWriter{openErr: errors.New("db connection failed")}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "open table restore writer")
	// reader.Open succeeded so reader.Close must be called via defer
	assert.Equal(t, 1, reader.closeCalled)
	// writer.Close defer was never registered
	assert.Equal(t, 0, writer.closeCalled)
}

func TestTableRestorer_Restore_ReadRowError(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{
		rows:    [][]byte{[]byte("(1)")},
		readErr: errors.New("corrupted data file"),
	}
	writer := &stubWriter{}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "read row")
	assert.Equal(t, 1, reader.closeCalled)
	assert.Equal(t, 1, writer.closeCalled)
}

func TestTableRestorer_Restore_WriteRowError(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{rows: [][]byte{[]byte("(1)"), []byte("(2)")}}
	writer := &stubWriter{writeErr: errors.New("insert rejected")}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "write row")
	assert.Equal(t, 1, reader.closeCalled)
	assert.Equal(t, 1, writer.closeCalled)
}

func TestTableRestorer_Restore_ReaderCloseError(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{closeErr: errors.New("reader close failed")}
	writer := &stubWriter{}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "close table restore reader")
}

func TestTableRestorer_Restore_WriterCloseError(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{}
	writer := &stubWriter{closeErr: errors.New("commit failed")}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "close table restore writer")
}

// TestTableRestorer_Restore_WriterCloseMasksReaderClose verifies the LIFO defer
// semantics: writer.Close runs first and sets retErr, so reader.Close error is
// silently dropped (retErr is already non-nil when reader.Close runs).
func TestTableRestorer_Restore_WriterCloseMasksReaderClose(t *testing.T) {
	table := testTable("db", "t", "id")
	reader := &stubReader{closeErr: errors.New("reader close")}
	writer := &stubWriter{closeErr: errors.New("writer close")}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.Error(t, err)
	assert.ErrorContains(t, err, "close table restore writer")
	assert.NotContains(t, err.Error(), "reader close")
}

// TestTableRestorer_Restore_RowsPreservedExactly checks that WriteRow receives
// the exact bytes that ReadRow returned, with no mutation.
func TestTableRestorer_Restore_RowsPreservedExactly(t *testing.T) {
	table := testTable("db", "t", "id", "data")
	rows := [][]byte{
		[]byte("(1,NULL)"),
		[]byte("(2,'hello world')"),
		[]byte("(3,'it''s fine')"),
	}
	reader := &stubReader{rows: rows}
	writer := &stubWriter{}
	r := NewTableRestorer(core.ObjectRestoreSpec{}, table, reader, writer)

	err := r.Restore(context.Background(), &stubSession{}, stubConnConfigurer{}, &stubStorager{})
	require.NoError(t, err)
	require.Len(t, writer.received, 3)
	for i, want := range rows {
		assert.Equal(t, want, writer.received[i], "row %d mismatch", i)
	}
}
