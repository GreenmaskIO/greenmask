package transformers

// RowDriver - represents methods for interacts with any transferring format
// It might be COPY, CSV, JSON, etc
// See implementation pgcopy.Row
// RowDriver must keep the current row state
type RowDriver interface {
	// GetColumn - get raw []byte value by column idx
	GetColumn(idx int) (*RawValue, error)
	// SetColumn - set RawValue value by column idx to the current row
	SetColumn(idx int, v *RawValue) error
	// Encode - encode the whole row to the []byte representation of RowDriver. It would be CSV
	// line or JSON object, etc.
	Encode() ([]byte, error)
	// Decode - decode current row state into slice of row values ([]*RawValue)
	Decode() ([]*RawValue, error)
}
