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
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	errReceivedRecordLengthMismatch = errors.New("received record length mismatch with expected")
	errUnexpectedColumn             = errors.New("unexpected column")
)

type CMDColumn interface {
	GetData() []byte
	IsValueNull() bool
	SetData([]byte)
	SetNull(bool)
}

type CMDColumnJson interface {
	CMDColumn
	Json()
}

// JsonRecordWithAttrIndexes - Record transferRecord transfer object for interaction with custom transformer via PIPE
type JsonRecordWithAttrIndexes[T CMDColumnJson] struct {
	// transferMap - map of column idx to position in record slice for transfer optimization.
	transferMap    map[int]int
	transferRecord []T
	// receiveMap - map of column idx to position in record slice for receive optimization.
	receiveMap    map[int]int
	receiveRecord []T
	newFn         func() T
}

func NewJsonRecordWithAttrIndexes[T CMDColumnJson](
	transfer []*ColumnMapping,
	receive []*ColumnMapping,
	newFn func() T,
) *JsonRecordWithAttrIndexes[T] {
	transferMap := make(map[int]int)
	for i := range transfer {
		columnsIdx := transfer[i].Column.Idx
		pos := transfer[i].Position
		transferMap[columnsIdx] = pos
	}
	receiveMap := make(map[int]int)
	for i := range receive {
		columnsIdx := receive[i].Column.Idx
		pos := receive[i].Position
		receiveMap[columnsIdx] = pos
	}
	return &JsonRecordWithAttrIndexes[T]{
		transferMap:    transferMap,
		receiveMap:     receiveMap,
		transferRecord: make([]T, len(transfer)),
		receiveRecord:  make([]T, len(receive)),
		newFn:          newFn,
	}
}

func (m *JsonRecordWithAttrIndexes[T]) Encode() ([]byte, error) {
	return json.Marshal(m.transferRecord)
}

func (m *JsonRecordWithAttrIndexes[T]) Decode(data []byte) error {
	m.receiveRecord = m.receiveRecord[:0]
	return json.Unmarshal(data, &m.receiveRecord)
}

func (m *JsonRecordWithAttrIndexes[T]) GetColumn(c *models.Column) (*models.ColumnRawValue, error) {
	pos, ok := m.receiveMap[c.Idx]
	if !ok {
		return nil, fmt.Errorf("column idx=%d name=%s: %w", c.Idx, c.Name, errUnexpectedColumn)
	}
	if pos >= len(m.receiveRecord) {
		return nil, fmt.Errorf("position %d is out of range: %w", pos, errReceivedRecordLengthMismatch)
	}
	res := m.receiveRecord[pos]
	return models.NewColumnRawValue(res.GetData(), res.IsValueNull()), nil
}

func (m *JsonRecordWithAttrIndexes[T]) SetColumn(c *models.Column, v *models.ColumnRawValue) error {
	pos, ok := m.transferMap[c.Idx]
	if !ok {
		return fmt.Errorf("column with idx=%d is not found", c.Idx)
	}
	if pos >= len(m.transferRecord) {
		return fmt.Errorf("position %d is out of range: %w", pos, errReceivedRecordLengthMismatch)
	}
	col := m.newFn()
	col.SetData(v.Data)
	col.SetNull(v.IsNull)
	m.transferRecord[pos] = col
	return nil
}

func (m *JsonRecordWithAttrIndexes[T]) Clean() {
	clear(m.transferRecord)
	clear(m.receiveRecord)
}
