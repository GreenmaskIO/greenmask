package validate

import (
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type CSVRecord struct {
	row [][]byte
}

func (c *CSVRecord) GetColumn(idx int) (*commonmodels.ColumnRawValue, error) {
	if idx < 0 || idx >= len(c.row) {
		return nil, nil
	}
	val := c.row[idx]
	if string(val) == "\\N" {
		return commonmodels.NewColumnRawValue(nil, true), nil
	}
	return commonmodels.NewColumnRawValue(val, false), nil
}

func (c *CSVRecord) SetColumn(int, *commonmodels.ColumnRawValue) error {
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
