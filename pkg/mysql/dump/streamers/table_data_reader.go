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
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

var (
	errConnectionWasNotOpened        = errors.New("connection was not opened")
	errDataChannelUnexpectedlyClosed = errors.New("data channel was not closed")
	errUnknownFieldType              = errors.New("unknown field type")
)

type TableDataReader struct {
	tx           pool.WorkerConn
	txPool       *pool.ConsistentTxPool
	columnLength int
	connConfig   *mysqlmodels.ConnConfig
	query        string
	eg           *errgroup.Group
	cancel       context.CancelFunc
	table        *models.Table
	// dataCh - actually stored data.
	dataCh chan [][]byte
	// endOfStreamCh - marks stream as completed successfully. Return ErrEndOfStream.
	errorCh chan error
}

func NewTableDataReader(
	table *models.Table,
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
		return val, nil
	default:
		return nil, fmt.Errorf("field type %d: %w", field.Type, errUnknownFieldType)
	}
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

func (r *TableDataReader) stream(ctx context.Context) func() error {
	return func() error {
		defer close(r.errorCh)
		var result mysql.Result
		recordData := make([][]byte, r.columnLength)
		var lineNum int64
		err := r.tx.RawConn().ExecuteSelectStreaming(r.query, &result, func(row []mysql.FieldValue) error {
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
			// TODO: Consider optimizing this with a buffer pool.
			case r.dataCh <- deepCopyRow(recordData):
			case <-ctx.Done():
				log.Ctx(ctx).Debug().
					Int("LineNum", int(lineNum)).
					Str("TableName", r.table.Name).
					Str("SchemaName", r.table.Schema).
					Err(ctx.Err()).
					Msg("data reader context done - ignore it in validate command")
				return errors.Join(models.ErrDumpStreamTerminated, ctx.Err())
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

func (r *TableDataReader) SetTxPool(txPool *pool.ConsistentTxPool) {
	r.txPool = txPool
}

func (r *TableDataReader) Open(ctx context.Context) error {
	if r.txPool == nil {
		return fmt.Errorf("transaction pool is not set")
	}
	tx, err := r.txPool.GetConn(ctx)
	if err != nil {
		return fmt.Errorf("get connection from pool: %w", err)
	}
	r.tx = tx

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
			return nil, models.ErrEndOfStream
		}
		return nil, fmt.Errorf("read row from channel: %w", err)
	case <-ctx.Done():
		return nil, fmt.Errorf("read row from channel: %w", ctx.Err())
	}
}

func (r *TableDataReader) Close(ctx context.Context) error {
	if r.tx == nil {
		return errConnectionWasNotOpened
	}
	r.cancel()
	err := r.eg.Wait()

	// Return connection to the pool
	if putErr := r.txPool.PutConn(ctx, r.tx); putErr != nil {
		log.Ctx(ctx).Warn().Err(putErr).Msg("failed to return connection to pool")
	}
	r.tx = nil

	if err != nil {
		return fmt.Errorf("stream reader exited with error: %w", err)
	}
	return nil
}

func (r *TableDataReader) DebugInfo() map[string]any {
	return map[string]any{
		models.MetaKeyTableName:      r.table.Name,
		models.MetaKeyTableSchema:    r.table.Schema,
		models.MetaKeyTableDumpQuery: r.query,
	}
}
