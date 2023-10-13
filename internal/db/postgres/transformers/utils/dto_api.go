package utils

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"io"
)

type DtoApi interface {
	// Skip - decide should be this record skipped or not. Useful for decreasing IO time and useless interaction
	Skip(r *toolkit.Record) bool
	// GetRowDriverFromRecord - get from toolkit.Record all the required attributes as a toolkit.RowDriver instance
	GetRowDriverFromRecord(r *toolkit.Record) (toolkit.RowDriver, error)
	// SetRowDriverToRecord - set transformed toolkit.RowDriver to the toolkit.Record
	SetRowDriverToRecord(rd toolkit.RowDriver, r *toolkit.Record) error
	// Encode - write encoded data ib []byte with \n symbol on the end into io.Writer
	Encode(row toolkit.RowDriver, w io.Writer) error
	// Unmarshal - unmarshall bytes data to toolkit.RowDriver
	Unmarshal(data []byte) (toolkit.RowDriver, error)
}
