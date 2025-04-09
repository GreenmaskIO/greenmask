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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type RecordContextReadWrite struct {
	*TableDriverContext
	*RecordContextReadOnly
	changedColumns map[string]struct{}
}

func NewRecordContextReadWrite() *RecordContextReadWrite {
	return &RecordContextReadWrite{
		TableDriverContext:    NewTableDriverContext(nil),
		RecordContextReadOnly: NewRecordContextReadOnly(),
	}
}

func (rc *RecordContextReadWrite) SetRecord(r commonininterfaces.Recorder) {
	rc.record = r
	rc.TableDriverContext.td = r.TableDriver()
}

func (rc *RecordContextReadWrite) GetChangedColumns() map[string]struct{} {
	return rc.changedColumns
}

func (rc *RecordContextReadWrite) Clean() {
	rc.record = nil
	for name := range rc.changedColumns {
		delete(rc.changedColumns, name)
	}
}

func (rc *RecordContextReadWrite) SetColumnValue(name string, v any) (bool, error) {
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

func (rc *RecordContextReadWrite) SetRawColumnValue(name string, v any) (bool, error) {
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
