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
	// SetRow - sets a row data directly to the RowDriver state.
	// This can be used to override the whole record or
	// to copy a data from driver if it has been provided already split
	// by columns. Can return error if the requested row to replace
	// len if not equal to the current.
	SetRow(row [][]byte) error
	GetRow() [][]byte
}
