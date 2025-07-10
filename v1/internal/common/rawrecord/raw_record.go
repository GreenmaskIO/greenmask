package rawrecord

import (
	"bytes"
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
)

type RawRecord struct {
	columnCount int
	row         [][]byte
	nullValue   []byte
}

func NewRawRecord(columnCount int, nullValue []byte) *RawRecord {
	return &RawRecord{
		columnCount: columnCount,
		row:         make([][]byte, columnCount),
		nullValue:   nullValue,
	}
}

func (c *RawRecord) GetColumn(idx int) (*commonmodels.ColumnRawValue, error) {
	if idx < 0 || idx >= c.columnCount {
		return nil, fmt.Errorf("column index %d out of range: %w", idx, commonmodels.ErrUnknownColumnIdx)
	}
	if bytes.Equal(c.nullValue, c.row[idx]) {
		return commonmodels.NewColumnRawValue(nil, true), nil
	}
	return commonmodels.NewColumnRawValue(c.row[idx], false), nil
}

func (c *RawRecord) SetColumn(idx int, v *commonmodels.ColumnRawValue) error {
	if idx < 0 || idx >= c.columnCount {
		return fmt.Errorf("column index %d out of range: %w", idx, commonmodels.ErrUnknownColumnIdx)
	}
	if v.IsNull {
		c.row[idx] = utils.CopyAndExtendIfNeeded(c.row[idx], c.nullValue)
		return nil
	}
	c.row[idx] = utils.CopyAndExtendIfNeeded(c.row[idx], v.Data)
	return nil
}

func (c *RawRecord) SetRow(row [][]byte) error {
	if len(row) != c.columnCount {
		return fmt.Errorf(
			"src length %d is not equal to dst length %d: %w",
			len(row), c.columnCount, commonmodels.ErrProvidedRowLengthIsNotEqualToTheDestination,
		)
	}
	for i := range c.row {
		// Copy from one row to another.
		// If the size of dst if greater than current allocate a new and then copy.
		c.row[i] = utils.CopyAndExtendIfNeeded(c.row[i], row[i])
	}
	return nil
}

func (c *RawRecord) GetRow() [][]byte {
	return c.row
}
