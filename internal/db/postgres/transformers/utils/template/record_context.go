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

package template

import (
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type RecordContext struct {
	record         *toolkit.Record
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

func (rc *RecordContext) SetRecord(r *toolkit.Record) {
	rc.record = r
}

func (rc *RecordContext) GetColumnType(name string) (string, error) {
	_, c, ok := rc.record.Driver.GetColumnByName(name)
	if !ok {
		return "", fmt.Errorf("column with name \"%s\" is not found", name)
	}
	return c.TypeName, nil
}

func (rc *RecordContext) GetValue(name string) (any, error) {
	v, err := rc.record.GetAttributeValueByName(name)
	if err != nil {
		return nil, err
	}
	if v.IsNull {
		return NullValue, nil
	}
	return v.Value, nil
}

func (rc *RecordContext) GetRawValue(name string) (any, error) {
	v, err := rc.record.GetRawAttributeValueByName(name)
	if err != nil {
		return nil, err
	}
	if v.IsNull {
		return NullValue, nil
	}
	return string(v.Data), nil
}

func (rc *RecordContext) SetValue(name string, v any) (bool, error) {
	var val *toolkit.Value
	switch v.(type) {
	case NullType:
		val = toolkit.NewValue(nil, true)
	default:
		val = toolkit.NewValue(v, false)
	}
	err := rc.record.SetAttributeValueByName(name, val)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (rc *RecordContext) SetRawValue(name string, v any) (bool, error) {
	var val *toolkit.RawValue
	switch vv := v.(type) {
	case NullType:
		val = toolkit.NewRawValue(nil, true)
	case string:
		val = toolkit.NewRawValue([]byte(vv), false)
	default:
		return false, fmt.Errorf("the raw value must be NullValue or string received %+v", vv)
	}
	err := rc.record.SetRawAttributeValueByName(name, val)
	if err != nil {
		return false, err
	}
	return true, nil
}

// EncodeValue - encode value from real type to the string or NullValue using column type
func (rc *RecordContext) EncodeValue(name string, v any) (any, error) {
	if _, ok := v.(NullType); ok {
		return NullValue, nil
	}

	res, err := rc.record.Driver.EncodeAttrName(name, v, nil)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// DecodeValue - decode value from string or NullValue to the real type using column type
func (rc *RecordContext) DecodeValue(name string, v any) (any, error) {
	switch vv := v.(type) {
	case NullType:
		return NullValue, nil
	case string:
		res, err := rc.record.Driver.DecodeAttrByName(name, []byte(vv))
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

	res, err := rc.record.Driver.EncodeByTypeName(name, v, nil)
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
		res, err := rc.record.Driver.DecodeByTypeName(name, []byte(vv))
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
