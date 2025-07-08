package interfaces

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

// RowDriver - represents methods for interacts with any transferring format
// It might be COPY, CSV, JSON, etc.
// See implementation pgcopy.Row
// RowDriver must keep the current row state
type RowDriver interface {
	// GetColumn - get raw []byte value by column idx
	GetColumn(idx int) (*models.ColumnRawValue, error)
	// SetColumn - set RawValue value by column idx to the current row
	SetColumn(idx int, v *models.ColumnRawValue) error
	// Encode - encode the whole row to the []byte representation of RowDriver. It would be CSV
	// line or JSON object, etc.
	Encode() ([]byte, error)
	// Decode - decode []bytes to RowDriver instance
	Decode([]byte) error
	// Length - count of attributes in the row
	Length() int
	// Clean - clean the state
	Clean()
}
