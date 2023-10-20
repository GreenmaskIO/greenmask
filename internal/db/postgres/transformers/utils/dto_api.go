package utils

import (
	"context"
	"io"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// InteractionApi - API for interaction with Cmd transformer. It must implement context cancellation, RW timeouts,
// encode-decode operations, extracting DTO and assigning received DTO to the toolkit.Record
type InteractionApi interface {
	// SetWriter - assign writer
	SetWriter(w io.Writer)
	// SetReader - assign reader
	SetReader(r io.Reader)
	// GetRowDriverFromRecord - get from toolkit.Record all the required attributes as a toolkit.RowDriver instance
	GetRowDriverFromRecord(r *toolkit.Record) (toolkit.RowDriver, error)
	// SetRowDriverToRecord - set transformed toolkit.RowDriver to the toolkit.Record
	SetRowDriverToRecord(rd toolkit.RowDriver, r *toolkit.Record) error
	// Encode - write encoded data with \n symbol in the end into io.Writer
	Encode(ctx context.Context, row toolkit.RowDriver) error
	// Decode - read data with new line from io.Reader and encode to toolkit.RowDriver
	Decode(ctx context.Context) (toolkit.RowDriver, error)
}
