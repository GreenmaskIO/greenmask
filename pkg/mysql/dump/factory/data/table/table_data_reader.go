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
	"errors"
	"fmt"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
)

var (
	errConnectionWasNotOpened        = errors.New("connection was not opened")
	errSessionWasNotSet              = errors.New("dump session is not set: call Init before Open")
	errUnexpectedEngineResource      = errors.New("unexpected engine resource type")
	errDataChannelUnexpectedlyClosed = errors.New("data channel was not closed")
	errUnknownFieldType              = errors.New("unknown field type")
	errRowColumnCountMismatch        = errors.New("row column count mismatch")
)

type TableDataReader struct {
	session      core.DatabaseSession
	columnLength int
	query        string
	eg           *errgroup.Group
	cancel       context.CancelFunc
	table        *core.Table
	// dataCh - actually stored data.
	dataCh chan [][]byte
	// endOfStreamCh - marks stream as completed successfully. Return ErrEndOfStream.
	errorCh chan error
}

func NewTableDataReader(
	table *core.Table,
	query string,
) *TableDataReader {
	if len(table.Columns) == 0 {
		panic("no columns in table")
	}
	if query == "" {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s`", table.Schema, table.Name)
	}
	return &TableDataReader{
		table:        table,
		query:        query,
		dataCh:       make(chan [][]byte),
		errorCh:      make(chan error, 1),
		columnLength: len(table.Columns),
	}
}

func fieldValueToString(field mysql.FieldValue) ([]byte, error) {
	switch field.Type {
	case mysql.FieldValueTypeNull:
		return mysqldbmsdriver.NullValueSeq, nil
	case mysql.FieldValueTypeUnsigned:
		val := field.AsUint64()
		return []byte(strconv.FormatUint(val, 10)), nil
	case mysql.FieldValueTypeSigned:
		val := field.AsInt64()
		return []byte(strconv.FormatInt(val, 10)), nil
	case mysql.FieldValueTypeFloat:
		val := field.AsFloat64()
		return []byte(strconv.FormatFloat(val, 'f', -1, 64)), nil
	case mysql.FieldValueTypeString:
		val := field.AsString()
		// If the string value is the same as the NULL sequence, we need to escape it
		// to avoid confusion with a real NULL value during record processing.
		if bytes.Equal(val, mysqldbmsdriver.NullValueSeq) {
			return escapeConflictingValue(val), nil
		}
		return val, nil
	default:
		return nil, fmt.Errorf("field type %d: %w", field.Type, errUnknownFieldType)
	}
}

func escapeConflictingValue(val []byte) []byte {
	escaped := make([]byte, len(val)+1)
	escaped[0] = '\\'
	copy(escaped[1:], val)
	return escaped
}

func deepCopyRow(row [][]byte) [][]byte {
	copied := make([][]byte, len(row))
	for i := range row {
		if row[i] != nil {
			copied[i] = make([]byte, len(row[i]))
			copy(copied[i], row[i])
		} else {
			copied[i] = nil
		}
	}
	return copied
}

// streamRows reads the table over the borrowed worker connection and pushes each
// row to dataCh until the result set is exhausted or ctx is cancelled.
func (r *TableDataReader) streamRows(ctx context.Context, conn pool.WorkerConn) error {
	var result mysql.Result
	recordData := make([][]byte, r.columnLength)
	var lineNum int64
	err := conn.RawConn().ExecuteSelectStreaming(r.query, &result, func(row []mysql.FieldValue) error {
		lineNum++
		if len(row) != r.columnLength {
			return fmt.Errorf("%w: expected %d, got %d at line %d for table %s",
				errRowColumnCountMismatch, r.columnLength, len(row), lineNum, r.table.Name)
		}
		for i := range row {
			v, err := fieldValueToString(row[i])
			if err != nil {
				return fmt.Errorf(`parse column "%s" value: %w`, string(result.Fields[i].Name), err)
			}
			recordData[i] = v
		}
		select {
		// We have to clone the recordData slice because it may be changed
		// when the next row is read and the data writing side is not yet ready.
		// We could optimize this, but it would require more complex logic.
		// TODO: Consider optimizing this with a buffer pool.
		case r.dataCh <- deepCopyRow(recordData):
		case <-ctx.Done():
			log.Ctx(ctx).Debug().
				Int("LineNum", int(lineNum)).
				Str("TableName", r.table.Name).
				Str("SchemaName", r.table.Schema).
				Err(ctx.Err()).
				Msg("data reader context done - ignore it in validate command")
			return errors.Join(core.ErrDumpStreamTerminated, ctx.Err())
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("stream table data at line %d: %w", lineNum, err)
	}
	return nil
}

// Open binds the dump session that owns the engine resources and opens the
// stream; the connection itself is borrowed lazily when streaming starts.
func (r *TableDataReader) Open(ctx context.Context, session core.DatabaseSession) error {
	if session == nil {
		return errSessionWasNotSet
	}
	r.session = session

	ctx, r.cancel = context.WithCancel(ctx)
	r.eg, ctx = errgroup.WithContext(ctx)
	// The borrow is scoped to this goroutine: RunWithEngineResource acquires a
	// pooled connection bound to the dump snapshot, streams over it, and returns
	// it to the pool when the callback exits (on EOF, error, or Close-triggered
	// cancellation). Any error — acquisition or streaming — is surfaced to the
	// consumer via errorCh.
	r.eg.Go(func() error {
		defer close(r.errorCh)
		err := r.session.RunWithEngineResource(ctx, func(ctx context.Context, res any) error {
			conn, ok := res.(pool.WorkerConn)
			if !ok {
				return fmt.Errorf("%w: %T", errUnexpectedEngineResource, res)
			}
			return r.streamRows(ctx, conn)
		})
		if err != nil {
			r.errorCh <- err
		}
		return err
	})
	return nil
}

func (r *TableDataReader) ReadRow(ctx context.Context) ([][]byte, error) {
	select {
	case data, ok := <-r.dataCh:
		if !ok {
			return nil, errDataChannelUnexpectedlyClosed
		}
		return data, nil
	case err, ok := <-r.errorCh:
		if !ok {
			// If the channel was closed with no items
			// then streamer exited successfully.
			return nil, core.ErrEndOfStream
		}
		return nil, fmt.Errorf("read row from channel: %w", err)
	case <-ctx.Done():
		return nil, fmt.Errorf("read row from channel: %w", ctx.Err())
	}
}

func (r *TableDataReader) Close(ctx context.Context) error {
	if r.cancel == nil {
		return errConnectionWasNotOpened
	}
	r.cancel()
	// Wait for the streaming goroutine to finish; RunWithConn returns the
	// connection to the pool as it unwinds.
	err := r.eg.Wait()
	r.cancel = nil

	if err != nil {
		return fmt.Errorf("stream reader exited with error: %w", err)
	}
	return nil
}

func (r *TableDataReader) DebugInfo() map[string]any {
	return map[string]any{
		core.MetaKeyTableName:      r.table.Name,
		core.MetaKeyTableSchema:    r.table.Schema,
		core.MetaKeyTableDumpQuery: r.query,
	}
}
