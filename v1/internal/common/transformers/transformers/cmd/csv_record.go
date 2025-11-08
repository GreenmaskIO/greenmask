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
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/csv"
)

var (
	NullSeq = []byte("\\N")
)

type CSVRecord struct {
	buf            *bytes.Buffer
	r              *csv.Reader
	w              *csv.Writer
	transferMap    map[int]int
	transferRecord [][]byte
	receiveMap     map[int]int
	receiveRecord  [][]byte
}

var (
	errNoReceiveColumnProvided = errors.New("no receive column provided")
)

func NewCSVRecord(
	transfer []*ColumnMapping,
	receive []*ColumnMapping,
) (*CSVRecord, error) {
	if len(receive) == 0 {
		return nil, errNoReceiveColumnProvided
	}
	buf := bytes.NewBuffer(nil)
	r := csv.NewReader(buf)
	r.ReuseRecord = true
	w := csv.NewWriter(buf)
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

	return &CSVRecord{
		buf:            buf,
		r:              r,
		w:              w,
		transferRecord: make([][]byte, len(transfer)),
		transferMap:    transferMap,
		receiveMap:     receiveMap,
		receiveRecord:  make([][]byte, len(receive)),
	}, nil
}

func (rr *CSVRecord) GetColumn(c *models.Column) (*models.ColumnRawValue, error) {
	csvIdx, ok := rr.receiveMap[c.Idx]
	if !ok {
		return nil, fmt.Errorf("column idx=%d name=%s: %w", c.Idx, c.Name, errUnexpectedColumn)
	}
	if csvIdx >= len(rr.receiveRecord) {
		return nil, fmt.Errorf("position %d is out of range", csvIdx)
	}
	val := rr.receiveRecord[csvIdx]

	if isNullSeq(val) {
		return models.NewColumnRawValue(nil, true), nil
	}
	return models.NewColumnRawValue(val, false), nil
}

func (rr *CSVRecord) SetColumn(c *models.Column, v *models.ColumnRawValue) error {
	csvIdx, ok := rr.transferMap[c.Idx]
	if !ok {
		return fmt.Errorf("column idx=%d name=%s: %w", c.Idx, c.Name, errUnexpectedColumn)
	}
	if csvIdx >= len(rr.transferRecord) {
		return fmt.Errorf("position %d is out of range", csvIdx)
	}
	if v.IsNull {
		rr.transferRecord[csvIdx] = slices.Clone(NullSeq)
	} else {
		rr.transferRecord[csvIdx] = v.Data
	}
	return nil
}

func (rr *CSVRecord) Encode() ([]byte, error) {
	var err error
	if err = rr.w.Write(rr.transferRecord); err != nil {
		return nil, fmt.Errorf("error writing to buf: %w", err)
	}
	rr.w.Flush()
	data, err := rr.buf.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("erro reading decoded bytes: %w", err)
	}
	return data, nil
}

func (rr *CSVRecord) Decode(data []byte) (err error) {
	rr.buf.Write(data)
	if rr.receiveRecord, err = rr.r.Read(); err != nil {
		return fmt.Errorf("error reading parsed csv: %w", err)
	}
	return nil
}

func (rr *CSVRecord) Clean() {
	clear(rr.transferRecord)
	clear(rr.receiveRecord)
}
