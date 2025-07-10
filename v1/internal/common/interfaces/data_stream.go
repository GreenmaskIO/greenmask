package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// RowStreamReader - represents a stream reader from DBMS.
type RowStreamReader interface {
	Open(ctx context.Context) error
	ReadRow(ctx context.Context) ([][]byte, error)
	Close(ctx context.Context) error
}

// RowStreamWriter -
type RowStreamWriter interface {
	Open(ctx context.Context) error
	WriteRow(ctx context.Context, row [][]byte) error
	Close(ctx context.Context) error
	// Stat - returns a statistic of written and compressed data
	// and some additional info.
	Stat() models.ObjectStat
}
