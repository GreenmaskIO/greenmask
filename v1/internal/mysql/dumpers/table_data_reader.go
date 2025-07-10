package tablestreamer

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"golang.org/x/sync/errgroup"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
)

var (
	errConnectionWasNotOpened        = errors.New("connection was not opened")
	errDataChannelUnexpectedlyClosed = errors.New("data channel was not closed")
)

type TableDataReader struct {
	conn         *client.Conn
	columnLength int
	connConfig   *mysqlmodels.ConnConfig
	query        string
	eg           *errgroup.Group
	cancel       context.CancelFunc
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
		panic("no query")
	}
	return &TableDataReader{
		query:        query,
		connConfig:   &connConfig,
		dataCh:       make(chan [][]byte),
		errorCh:      make(chan error, 1),
		columnLength: len(table.Columns),
	}
}

func (r *TableDataReader) stream(ctx context.Context) func() error {
	return func() error {
		defer close(r.errorCh)
		var result mysql.Result
		recordData := make([][]byte, r.columnLength)
		err := r.conn.ExecuteSelectStreaming(r.query, &result, func(row []mysql.FieldValue) error {
			for i := range row {
				recordData = append(recordData, row[i].AsString())
			}
			select {
			case r.dataCh <- recordData:
			case <-ctx.Done():
				return fmt.Errorf("write row into channel: %w", ctx.Err())
			}
			return nil
		}, nil)
		if err != nil {
			r.errorCh <- err
			return fmt.Errorf("stream table data: %w", err)
		}
		return nil
	}
}

func (r *TableDataReader) Open(ctx context.Context) error {
	var err error
	r.conn, err = client.ConnectWithContext(
		ctx, r.connConfig.Address, r.connConfig.User,
		r.connConfig.Password, r.connConfig.DbName, r.connConfig.Timeout,
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
		if ok {
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
