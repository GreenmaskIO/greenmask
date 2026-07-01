package validate

import (
	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

type CSVRecord struct {
	row [][]byte
}

func (c *CSVRecord) GetColumn(idx int) (*core.ColumnRawValue, error) {
	if idx < 0 || idx >= len(c.row) {
		return nil, nil
	}
	val := c.row[idx]
	if string(val) == "\\N" {
		return core.NewColumnRawValue(nil, true), nil
	}
	return core.NewColumnRawValue(val, false), nil
}

func (c *CSVRecord) SetColumn(int, *core.ColumnRawValue) error {
	//TODO implement me
	panic("implement me")
}

func (c *CSVRecord) SetRow(row [][]byte) error {
	c.row = row
	return nil
}

func (c *CSVRecord) GetRow() [][]byte {
	panic("implement me")
}
