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

package toolkit

import (
	"fmt"
)

type Tuple map[string]*Value

type Record struct {
	Driver *Driver
	Row    RowDriver
	// rawValuesCache - needs for avoiding new RawValue creation when calling SetFunction
	rawValuesCache []*RawValue
}

func NewRecord(driver *Driver) *Record {
	rawValuesCache := make([]*RawValue, len(driver.Table.Columns))
	for idx := range rawValuesCache {
		rawValuesCache[idx] = NewRawValue(nil, false)
	}
	return &Record{
		Driver:         driver,
		rawValuesCache: rawValuesCache,
	}
}

func (r *Record) SetRow(row RowDriver) {
	r.Row = row
}

func (r *Record) GetTuple() (Tuple, error) {
	tuple := make(Tuple, len(r.Driver.Table.Columns))
	for _, c := range r.Driver.Table.Columns {
		v, err := r.GetColumnValueByName(c.Name)
		if err != nil {
			return nil, fmt.Errorf("error getting attribute: %w", err)
		}
		tuple[c.Name] = v
	}
	return tuple, nil
}

// ScanColumnValueByIdx - scan data from column with name into v and return isNull property and error
func (r *Record) ScanColumnValueByIdx(idx int, v any) (bool, error) {
	rawData, err := r.Row.GetColumn(idx)
	if err != nil {
		return false, err
	}

	if rawData.IsNull {
		return true, nil
	} else {
		if err := r.Driver.ScanValueByColumnIdx(idx, rawData.Data, v); err != nil {
			return false, fmt.Errorf("cannot scan: %w", err)
		}
	}
	return false, nil
}

func (r *Record) ScanColumnValueByName(name string, v any) (bool, error) {
	idx, c, ok := r.Driver.GetColumnByName(name)
	if !ok {
		return false, fmt.Errorf(`unknown column name "%s"`, name)
	}
	isNull, err := r.ScanColumnValueByIdx(idx, v)
	if err != nil {
		return false, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.Driver.Table.Schema, r.Driver.Table.Name, c.Name,
			err,
		)
	}
	return isNull, nil
}

func (r *Record) GetColumnValueByIdx(idx int) (*Value, error) {
	rawData, err := r.Row.GetColumn(idx)
	if err != nil {
		return nil, err
	}
	if rawData.IsNull {
		return NewValue(nil, true), nil
	}

	col := r.Driver.Table.Columns[idx]

	// For text-typed columns (varchar, text, char, name), return the raw bytes
	// as a string to preserve exact content including leading zeros.
	// This prevents numeric-looking strings from being interpreted as numbers.
	if isTextTypeOid(col.TypeOid) && col.OverriddenTypeOid == 0 {
		return NewValue(string(rawData.Data), false), nil
	}

	decodedValue, err := r.Driver.DecodeValueByColumnIdx(idx, rawData.Data)
	if err != nil {
		return nil, fmt.Errorf("error decoding arribute: %w", err)
	}
	return NewValue(decodedValue, false), nil
}

func (r *Record) GetColumnValueByName(name string) (*Value, error) {
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return nil, fmt.Errorf(`unknown column name "%s"`, name)
	}
	v, err := r.GetColumnValueByIdx(idx)
	if err != nil {
		return nil, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.Driver.Table.Schema, r.Driver.Table.Name, name,
			err,
		)
	}
	return v, nil
}

func (r *Record) SetColumnValueByIdx(idx int, v any) error {
	var value *Value
	switch vv := v.(type) {
	case *Value:
		value = vv
	default:
		value = NewValue(v, false)
	}
	if value.IsNull {
		rv := r.rawValuesCache[idx]
		rv.IsNull = true
		rv.Data = nil
		if err := r.Row.SetColumn(idx, rv); err != nil {
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
		if err = r.Row.SetColumn(idx, rv); err != nil {
			return fmt.Errorf("error setting column value in RowDriver: %w", err)
		}
	}

	return nil
}

// SetColumnValueByName - set transformed attribute to the tuple
func (r *Record) SetColumnValueByName(name string, v any) error {
	if v == nil {
		return fmt.Errorf("value cannot be nil pointer")
	}
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return fmt.Errorf("unable to find column by name")
	}

	return r.SetColumnValueByIdx(idx, v)
}

func (r *Record) Encode() (RowDriver, error) {
	return r.Row, nil
}

func (r *Record) encodeValue(idx int, v any) (res []byte, err error) {
	col := r.Driver.Table.Columns[idx]

	// If the column has a type override, always use Driver encoding to ensure
	// proper type handling (e.g., preserving leading zeros in varchar)
	if col.OverriddenTypeOid != 0 {
		res, err = r.Driver.EncodeValueByColumnIdx(idx, v, nil)
		if err != nil {
			return nil, fmt.Errorf("encoding error: %w", err)
		}
		return res, nil
	}

	// For text-typed columns (varchar, text, char, name), convert the value to
	// its string representation. This handles cases where a numeric value is
	// being written to a text column.
	// OIDs: 25=text, 1042=bpchar, 1043=varchar, 19=name
	if isTextTypeOid(col.TypeOid) {
		switch vv := v.(type) {
		case string:
			// String to text column - direct byte conversion preserves content exactly
			res = []byte(vv)
		default:
			// Non-string to text column - convert to string representation
			// This allows writing numeric values to text columns
			res = []byte(fmt.Sprintf("%v", vv))
		}
		return res, nil
	}

	switch vv := v.(type) {
	case string:
		res = []byte(vv)
	default:
		res, err = r.Driver.EncodeValueByColumnIdx(idx, vv, nil)
		if err != nil {
			return nil, fmt.Errorf("encoding error: %w", err)
		}
	}
	return res, nil
}

// isTextTypeOid returns true if the OID represents a text-based type
// where content should be preserved as-is without numeric interpretation
func isTextTypeOid(oid Oid) bool {
	// PostgreSQL OIDs for text types:
	// 25 = text, 1042 = bpchar (char), 1043 = varchar, 19 = name
	return oid == 25 || oid == 1042 || oid == 1043 || oid == 19
}

func (r *Record) GetRawColumnValueByName(name string) (*RawValue, error) {
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("unable to find column by name")
	}
	return r.Row.GetColumn(idx)
}

func (r *Record) GetRawColumnValueByIdx(idx int) (*RawValue, error) {
	return r.Row.GetColumn(idx)
}

func (r *Record) SetRawColumnValueByName(name string, value *RawValue) error {
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return fmt.Errorf("unable to find column by name")
	}
	if err := r.Row.SetColumn(idx, value); err != nil {
		return fmt.Errorf("error setting raw atribute value: %w", err)
	}
	return nil
}

func (r *Record) SetRawColumnValueByIdx(idx int, value *RawValue) error {
	if err := r.Row.SetColumn(idx, value); err != nil {
		return fmt.Errorf("error setting raw atribute value: %w", err)
	}
	return nil
}
