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

package cmd

import (
	"encoding/json"
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type JsonRecordWithAttrNames[T CMDColumnJson] struct {
	record map[string]T
	newFn  func() T
}

func NewJsonRecordWithAttrNames[T CMDColumnJson](newFn func() T) *JsonRecordWithAttrNames[T] {
	return &JsonRecordWithAttrNames[T]{
		record: make(map[string]T),
		newFn:  newFn,
	}
}

func (rr *JsonRecordWithAttrNames[T]) Encode() ([]byte, error) {
	return json.Marshal(rr.record)
}

func (rr *JsonRecordWithAttrNames[T]) Decode(data []byte) error {
	return json.Unmarshal(data, &rr.record)
}

func (rr *JsonRecordWithAttrNames[T]) GetColumn(c *commonmodels.Column) (*commonmodels.ColumnRawValue, error) {
	res, ok := rr.record[c.Name]
	if !ok {
		return nil, fmt.Errorf("attribute with name=%s is not found", c.Name)
	}
	return commonmodels.NewColumnRawValue(res.GetData(), res.IsValueNull()), nil
}

func (rr *JsonRecordWithAttrNames[T]) SetColumn(c *commonmodels.Column, v *commonmodels.ColumnRawValue) error {
	col := rr.newFn()
	col.SetData(v.Data)
	col.SetNull(v.IsNull)
	rr.record[c.Name] = col
	return nil
}

func (rr *JsonRecordWithAttrNames[T]) Clean() {
	for key := range rr.record {
		delete(rr.record, key)
	}
}
