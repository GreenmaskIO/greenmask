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
	"bytes"
	"encoding/csv"
	"fmt"

	"github.com/rs/zerolog/log"
)

const nullSeqStr = "\\N"

type RawRecordCsv struct {
	buf                  *bytes.Buffer
	r                    *csv.Reader
	w                    *csv.Writer
	Data                 []string
	size                 int
	columnRemap          []*Column
	originalIdxToOrdered map[int]int
}

func NewRawRecordCsv(size int, columnRemap []*Column) *RawRecordCsv {
	buf := bytes.NewBuffer(nil)
	r := csv.NewReader(buf)
	r.ReuseRecord = true
	w := csv.NewWriter(buf)
	var data []string
	originalIdxToOrdered := make(map[int]int)
	if len(columnRemap) > 0 {
		data = make([]string, len(columnRemap))
		for idx, c := range columnRemap {
			originalIdxToOrdered[c.Idx] = idx
		}
	} else {
		data = make([]string, size)
	}

	return &RawRecordCsv{
		buf:                  buf,
		r:                    r,
		w:                    w,
		size:                 size,
		Data:                 data,
		columnRemap:          columnRemap,
		originalIdxToOrdered: originalIdxToOrdered,
	}
}

func (rr *RawRecordCsv) GetColumn(idx int) (*RawValue, error) {
	var err error
	if rr.Data == nil {
		rr.Data, err = rr.r.Read()
		if err != nil {
			return nil, fmt.Errorf("error parsing csv record: %w", err)
		}
	}
	if idx > rr.size || idx < 0 {
		return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	var ok bool
	if len(rr.originalIdxToOrdered) > 0 {
		idx, ok = rr.originalIdxToOrdered[idx]
		if !ok {
			return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
		}
	}

	val := rr.Data[idx]

	if val == nullSeqStr {
		return NewRawValue(nil, true), nil
	}
	return NewRawValue([]byte(val), false), nil
}

func (rr *RawRecordCsv) SetColumn(idx int, v *RawValue) error {
	if idx > rr.size || idx < 0 {
		return fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	var ok bool
	if len(rr.originalIdxToOrdered) > 0 {
		idx, ok = rr.originalIdxToOrdered[idx]
		if !ok {
			return fmt.Errorf("attribute with idx=%d is not found", idx)
		}
	}

	if v.IsNull {
		rr.Data[idx] = nullSeqStr
	}
	rr.Data[idx] = string(v.Data)
	return nil
}

func (rr *RawRecordCsv) Encode() ([]byte, error) {
	var err error
	if err = rr.w.Write(rr.Data); err != nil {
		return nil, fmt.Errorf("error writing to buf: %w", err)
	}
	rr.w.Flush()
	data, err := rr.buf.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("erro reading decoded bytes: %w", err)
	}
	log.Debug().Str("data", string(data)).Msg("debug encoded csv")
	return data, nil
}

func (rr *RawRecordCsv) Decode(data []byte) (err error) {
	rr.buf.Write(data)
	if rr.Data, err = rr.r.Read(); err != nil {
		return fmt.Errorf("error reading parsed csv: %w", err)
	}
	return nil
}

func (rr *RawRecordCsv) Length() int {
	return len(rr.Data)
}

func (rr *RawRecordCsv) Clean() {
	//rr.Data = rr.Data[:0]
	clear(rr.Data)
}
