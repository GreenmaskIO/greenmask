package transformers

import (
	"errors"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type DummyRow struct {
	data []*commonmodels.ColumnRawValue
}

func NewDummyRow(numCols int) *DummyRow {
	if numCols <= 0 {
		panic("number of Columns should be greater than zero")
	}
	return &DummyRow{data: make([]*commonmodels.ColumnRawValue, numCols)}
}

func (d *DummyRow) GetColumn(idx int) (*commonmodels.ColumnRawValue, error) {
	if idx < 0 || idx >= len(d.data) {
		return nil, errors.New("index out of range")
	}
	return d.data[idx], nil
}

func (d *DummyRow) SetColumn(idx int, v *commonmodels.ColumnRawValue) error {
	if idx < 0 || idx >= len(d.data) {
		return errors.New("index out of range")
	}
	d.data[idx] = v
	return nil
}

func (d *DummyRow) SetRowRawColumnValue(row []*commonmodels.ColumnRawValue) {
	if len(row) != len(d.data) {
		panic("row length does not match")
	}
	for i := range row {
		d.data[i] = row[i]
	}
}

func (d *DummyRow) SetRow(_ [][]byte) error {
	panic("implement me")
}

func (d *DummyRow) GetRow() [][]byte {
	panic("implement me")
}
