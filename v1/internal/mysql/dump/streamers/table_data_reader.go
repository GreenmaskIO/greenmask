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
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"golang.org/x/sync/errgroup"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
)

var (
	errConnectionWasNotOpened        = errors.New("connection was not opened")
	errDataChannelUnexpectedlyClosed = errors.New("data channel was not closed")
	errCannotAssertType              = errors.New("cannot assert type")
	errUnknownFieldType              = errors.New("unknown field type")
)

type TableDataReader struct {
	conn         *client.Conn
	columnLength int
	connConfig   *mysqlmodels.ConnConfig
	query        string
	eg           *errgroup.Group
	cancel       context.CancelFunc
	table        *commonmodels.Table
	// dataCh - actually stored data.
	dataCh chan [][]byte
	// endOfStreamCh - marks stream as completed successfully. Return ErrEndOfStream.
	errorCh chan error
}

func NewTableDataReader(
	table *commonmodels.Table,
	connConfig mysqlmodels.ConnConfig,
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
		connConfig:   &connConfig,
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
		val, ok := field.Value().(uint64)
		if !ok {
			return nil, fmt.Errorf(
				"invalid type %T, expected uint64: %w", field.Value(), errCannotAssertType,
			)
		}
		return []byte(strconv.FormatUint(val, 10)), nil
	case mysql.FieldValueTypeSigned:
		val, ok := field.Value().(int64)
		if !ok {
			return nil, fmt.Errorf(
				"invalid type %T, expected int64: %w", field.Value(), errCannotAssertType,
			)
		}
		return []byte(strconv.FormatInt(val, 10)), nil
	case mysql.FieldValueTypeFloat:
		val, ok := field.Value().(int64)
		if !ok {
			return nil, fmt.Errorf(
				"invalid type %T, expected uint64: %w", field.Value(), errCannotAssertType,
			)
		}
		return []byte(strconv.FormatInt(val, 10)), nil
	case mysql.FieldValueTypeString:
		val, ok := field.Value().([]byte)
		if !ok {
			return nil, fmt.Errorf(
				"invalid type %T, expected []byte: %w", field.Value(), errCannotAssertType,
			)
		}
		return val, nil
	default:
		return nil, fmt.Errorf("field type %d: %w", field.Type, errUnknownFieldType)
	}
}

func (r *TableDataReader) stream(ctx context.Context) func() error {
	return func() error {
		defer close(r.errorCh)
		var result mysql.Result
		recordData := make([][]byte, r.columnLength)
		var lineNum int64
		err := r.conn.ExecuteSelectStreaming(r.query, &result, func(row []mysql.FieldValue) error {
			lineNum++
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
			case r.dataCh <- slices.Clone(recordData):
			case <-ctx.Done():
				return fmt.Errorf("write row into channel: %w", ctx.Err())
			}
			return nil
		}, nil)
		if err != nil {
			r.errorCh <- err
			return fmt.Errorf("stream table data at line %d: %w", lineNum, err)
		}
		return nil
	}
}

func (r *TableDataReader) Open(ctx context.Context) error {
	var err error
	r.conn, err = client.ConnectWithContext(
		ctx, r.connConfig.Address(), r.connConfig.User,
		r.connConfig.Password, r.connConfig.Database, r.connConfig.Timeout,
	)
	if err != nil {
		return fmt.Errorf("connect to mysql server: %w", err)
	}
	ctx, r.cancel = context.WithCancel(ctx)
	r.eg, ctx = errgroup.WithContext(ctx)
	r.eg.Go(r.stream(ctx))
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
			return nil, commonmodels.ErrEndOfStream
		}
		return nil, fmt.Errorf("read row from channel: %w", err)
	case <-ctx.Done():
		return nil, fmt.Errorf("read row from channel: %w", ctx.Err())
	}
}

func (r *TableDataReader) Close(_ context.Context) error {
	if r.conn == nil {
		return errConnectionWasNotOpened
	}
	r.cancel()
	if err := r.eg.Wait(); err != nil {
		return fmt.Errorf("stream reader exited with error: %w", err)
	}
	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("close mysql connection: %w", err)
	}
	return nil
}

func (r *TableDataReader) DebugInfo() map[string]any {
	return map[string]any{
		commonmodels.MetaKeyTableName:      r.table.Name,
		commonmodels.MetaKeyTableSchema:    r.table.Schema,
		commonmodels.MetaKeyTableDumpQuery: r.query,
	}
}
