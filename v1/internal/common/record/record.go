// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record

import (
	"errors"
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	errValueCannotBeNil = errors.New("value cannot be nil")
)

var (
	_ commonininterfaces.Recorder = (*Record)(nil)
)

type Tuple map[string]*commonmodels.ColumnValue

type Record struct {
	// tableDriver - it's a driver that was initialized with table data like columns.
	// This driver can encode/decode/scan value from raw data []byte into scalar
	// type (like int, str, etc.) and vice versa.
	tableDriver commonininterfaces.TableDriver
	// row - low level interface to address to the raw value of column in the row (tuple).
	// It simply can get/set value and encode/decode from/into raw row data (like CSV row).
	row commonininterfaces.RowDriver
	// rawValuesCache - needs for avoiding new RawValue creation when calling SetFunction
	rawValuesCache []*commonmodels.ColumnRawValue
}

func NewRecord(
	row commonininterfaces.RowDriver,
	tableDriver commonininterfaces.TableDriver,
) *Record {
	rawValuesCache := make([]*commonmodels.ColumnRawValue, len(tableDriver.Table().Columns))
	for idx := range rawValuesCache {
		rawValuesCache[idx] = commonmodels.NewColumnRawValue(nil, false)
	}
	return &Record{
		row:            row,
		tableDriver:    tableDriver,
		rawValuesCache: rawValuesCache,
	}
}

func (r *Record) IsNullByColumnName(columName string) (bool, error) {
	v, err := r.GetRawColumnValueByName(columName)
	if err != nil {
		return false, err
	}
	return v.IsNull, nil
}

func (r *Record) IsNullByColumnIdx(columIdx int) (bool, error) {
	v, err := r.GetRawColumnValueByIdx(columIdx)
	if err != nil {
		return false, err
	}
	return v.IsNull, nil
}

func (r *Record) GetColumnByName(columnName string) (*commonmodels.Column, error) {
	return r.tableDriver.GetColumnByName(columnName)
}

func (r *Record) TableDriver() commonininterfaces.TableDriver {
	return r.tableDriver
}

func (r *Record) SetRow(rawRecord [][]byte) error {
	return r.row.SetRow(rawRecord)
}

func (r *Record) GetRow() [][]byte {
	return r.row.GetRow()
}

func (r *Record) GetTuple() (Tuple, error) {
	tuple := make(Tuple, len(r.tableDriver.Table().Columns))
	for i := range r.tableDriver.Table().Columns {
		column := r.tableDriver.Table().Columns[i]
		v, err := r.GetColumnValueByName(column.Name)
		if err != nil {
			return nil, fmt.Errorf("error getting attribute: %w", err)
		}
		tuple[column.Name] = v
	}
	return tuple, nil
}

// ScanColumnValueByIdx - scan data from column with name into v and return isNull property and error
func (r *Record) ScanColumnValueByIdx(idx int, v any) (bool, error) {
	rawData, err := r.row.GetColumn(idx)
	if err != nil {
		return false, err
	}

	if rawData.IsNull {
		return true, nil
	} else {
		if err := r.tableDriver.ScanValueByColumnIdx(idx, rawData.Data, v); err != nil {
			return false, fmt.Errorf("cannot scan: %w", err)
		}
	}
	return false, nil
}

func (r *Record) ScanColumnValueByName(name string, v any) (bool, error) {
	idx, err := r.tableDriver.GetColumnIdxByName(name)
	if err != nil {
		return false, err
	}
	isNull, err := r.ScanColumnValueByIdx(idx, v)
	if err != nil {
		return false, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.tableDriver.Table().Schema, r.tableDriver.Table().Name, name,
			err,
		)
	}
	return isNull, nil
}

func (r *Record) GetColumnValueByIdx(idx int) (*commonmodels.ColumnValue, error) {
	rawData, err := r.row.GetColumn(idx)
	if err != nil {
		return nil, err
	}
	if rawData.IsNull {
		return commonmodels.NewColumnValue(nil, true), nil
	}
	decodedValue, err := r.tableDriver.DecodeValueByColumnIdx(idx, rawData.Data)
	if err != nil {
		return nil, fmt.Errorf("error decoding arribute: %w", err)
	}
	return commonmodels.NewColumnValue(decodedValue, false), nil
}

func (r *Record) GetColumnValueByName(name string) (*commonmodels.ColumnValue, error) {
	idx, err := r.tableDriver.GetColumnIdxByName(name)
	if err != nil {
		return nil, err
	}
	v, err := r.GetColumnValueByIdx(idx)
	if err != nil {
		return nil, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.tableDriver.Table().Schema, r.tableDriver.Table().Name, name,
			err,
		)
	}
	return v, nil
}

func (r *Record) encodeValue(idx int, v any) (res []byte, err error) {
	switch vv := v.(type) {
	case string:
		res = []byte(vv)
	default:
		res, err = r.tableDriver.EncodeValueByColumnIdx(idx, vv, nil)
		if err != nil {
			return nil, fmt.Errorf("encoding error: %w", err)
		}
	}
	return res, nil
}

func (r *Record) SetColumnValueByIdx(idx int, v any) error {
	var value *commonmodels.ColumnValue
	switch vv := v.(type) {
	case *commonmodels.ColumnValue:
		value = vv
	default:
		value = commonmodels.NewColumnValue(v, false)
	}
	if value.IsNull {
		rv := r.rawValuesCache[idx]
		rv.IsNull = true
		rv.Data = nil
		if err := r.row.SetColumn(idx, rv); err != nil {
			return fmt.Errorf("error setting column value in RowDriver: %w", err)
		}
	} else {
		encodedValue, err := r.encodeValue(idx, value.Value)
		if err != nil {
			return fmt.Errorf("unable to encode attr value: %w", err)
		}
		rv := r.rawValuesCache[idx]
		rv.IsNull = false
		rv.Data = encodedValue
		if err = r.row.SetColumn(idx, rv); err != nil {
			return fmt.Errorf("error setting column value in RowDriver: %w", err)
		}
	}

	return nil
}

// SetColumnValueByName - set transformed attribute to the tuple
func (r *Record) SetColumnValueByName(name string, v any) error {
	if v == nil {
		return errValueCannotBeNil
	}
	idx, err := r.tableDriver.GetColumnIdxByName(name)
	if err != nil {
		return err
	}

	return r.SetColumnValueByIdx(idx, v)
}

func (r *Record) GetRawColumnValueByName(name string) (*commonmodels.ColumnRawValue, error) {
	idx, err := r.tableDriver.GetColumnIdxByName(name)
	if err != nil {
		return nil, err
	}
	return r.row.GetColumn(idx)
}

func (r *Record) GetRawColumnValueByIdx(idx int) (*commonmodels.ColumnRawValue, error) {
	return r.row.GetColumn(idx)
}

func (r *Record) SetRawColumnValueByName(name string, value *commonmodels.ColumnRawValue) error {
	idx, err := r.tableDriver.GetColumnIdxByName(name)
	if err != nil {
		return nil
	}
	if err := r.row.SetColumn(idx, value); err != nil {
		return fmt.Errorf("error setting raw atribute value: %w", err)
	}
	return nil
}

func (r *Record) SetRawColumnValueByIdx(idx int, value *commonmodels.ColumnRawValue) error {
	if err := r.row.SetColumn(idx, value); err != nil {
		return fmt.Errorf("error setting raw atribute value: %w", err)
	}
	return nil
}
