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
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
)

// RecordContextReadOnly allows to perform only reading operation. Changes of record are not allowed.
type RecordContextReadOnly struct {
	*TableDriverContext
	record commonininterfaces.Recorder
}

func NewRecordContextReadOnly() *RecordContextReadOnly {
	return &RecordContextReadOnly{}
}

func (rc *RecordContextReadOnly) SetRecord(r commonininterfaces.Recorder) {
	rc.record = r
	rc.TableDriverContext = NewTableDriverContext(r.TableDriver())
}

func (rc *RecordContextReadOnly) GetColumnValue(name string) (any, error) {
	v, err := rc.record.GetColumnValueByName(name)
	if err != nil {
		return nil, err
	}
	if v.IsNull {
		return NullValue, nil
	}
	return v.Value, nil
}

func (rc *RecordContextReadOnly) GetRawColumnValue(name string) (any, error) {
	v, err := rc.record.GetRawColumnValueByName(name)
	if err != nil {
		return nil, err
	}
	if v.IsNull {
		return NullValue, nil
	}
	return string(v.Data), nil
}
