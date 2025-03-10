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

package parameters

import (
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type record interface {
	GetRawColumnValueByIdx(columnIdx int) (*models.ColumnRawValue, error)
	GetColumnValueByIdx(columnIdx int) (*models.ColumnValue, error)
	GetColumnValueByName(columnName string) (*models.ColumnValue, error)
	GetRawColumnValueByName(columnName string) (*models.ColumnRawValue, error)
	SetColumnValueByIdx(columnIdx int, v any) error
	SetRawColumnValueByIdx(columnIdx int, value *models.ColumnRawValue) error
	SetColumnValueByName(columnName string, v any) error
	SetRawColumnValueByName(columnName string, value *models.ColumnRawValue) error
	GetColumnByName(columnName string) (*models.Column, bool)
	Driver() driver
}

type RecordContext struct {
	record         record
	changedColumns map[string]struct{}
}

func NewRecordContext() *RecordContext {
	return &RecordContext{}
}

func (rc *RecordContext) GetChangedColumns() map[string]struct{} {
	return rc.changedColumns
}

func (rc *RecordContext) Clean() {
	rc.record = nil
	for name := range rc.changedColumns {
		delete(rc.changedColumns, name)
	}
}

func (rc *RecordContext) SetRecord(r record) {
	rc.record = r
}

func (rc *RecordContext) GetColumnType(name string) (string, error) {
	c, ok := rc.record.GetColumnByName(name)
	if !ok {
		return "", fmt.Errorf("column with name \"%s\" is not found", name)
	}
	return c.Type, nil
}

func (rc *RecordContext) GetColumnValue(name string) (any, error) {
	v, err := rc.record.GetColumnValueByName(name)
	if err != nil {
		return nil, err
	}
	if v.IsNull {
		return NullValue, nil
	}
	return v.Value, nil
}

func (rc *RecordContext) GetRawColumnValue(name string) (any, error) {
	v, err := rc.record.GetRawColumnValueByName(name)
	if err != nil {
		return nil, err
	}
	if v.IsNull {
		return NullValue, nil
	}
	return string(v.Data), nil
}

func (rc *RecordContext) SetColumnValue(name string, v any) (bool, error) {
	var val *models.ColumnValue
	switch v.(type) {
	case NullType:
		val = models.NewColumnValue(nil, true)
	default:
		val = models.NewColumnValue(v, false)
	}
	err := rc.record.SetColumnValueByName(name, val)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (rc *RecordContext) SetRawColumnValue(name string, v any) (bool, error) {
	var val *models.ColumnRawValue
	switch vv := v.(type) {
	case NullType:
		val = models.NewColumnRawValue(nil, true)
	case string:
		val = models.NewColumnRawValue([]byte(vv), false)
	default:
		return false, fmt.Errorf("the raw value must be NullValue or string received %+v", vv)
	}
	err := rc.record.SetRawColumnValueByName(name, val)
	if err != nil {
		return false, err
	}
	return true, nil
}

// EncodeValueByColumn - encode value from real type to the string or NullValue using column type
func (rc *RecordContext) EncodeValueByColumn(name string, v any) (any, error) {
	if _, ok := v.(NullType); ok {
		return NullValue, nil
	}

	res, err := rc.record.Driver().EncodeValueByColumnName(name, v, nil)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// DecodeValueByColumn - decode value from string or NullValue to the real type using column type
func (rc *RecordContext) DecodeValueByColumn(name string, v any) (any, error) {
	switch vv := v.(type) {
	case NullType:
		return NullValue, nil
	case string:
		res, err := rc.record.Driver.DecodeValueByColumnName(name, []byte(vv))
		if err != nil {
			return nil, err
		}
		return castToDefault(res), nil
	default:
		return "", fmt.Errorf("unable to decode value %+v by column  \"%s\"", vv, name)
	}
}

// EncodeValueByType - encode value from real type to the string or NullValue using type
func (rc *RecordContext) EncodeValueByType(name string, v any) (any, error) {
	realName, ok := typeAliases[name]
	if ok {
		name = realName
	}

	if _, ok := v.(NullType); ok {
		return NullValue, nil
	}

	res, err := rc.record.Driver.EncodeValueByTypeName(name, v, nil)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// DecodeValueByType - decode value from string or NullValue to the real type using type
func (rc *RecordContext) DecodeValueByType(name string, v any) (any, error) {
	realName, ok := typeAliases[name]
	if ok {
		name = realName
	}

	switch vv := v.(type) {
	case NullType:
		return NullValue, nil
	case string:
		res, err := rc.record.Driver.DecodeValueByTypeName(name, []byte(vv))
		if err != nil {
			return nil, err
		}
		return castToDefault(res), nil
	default:
		return "", fmt.Errorf("unable to decode value %+v by type \"%s\"", vv, name)
	}
}

func castToDefault(v any) any {
	switch vv := v.(type) {
	case int16:
		return int64(vv)
	case int32:
		return int64(vv)
	case float32:
		return float64(vv)
	}
	return v
}
