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
	"io"

	"github.com/rs/zerolog/log"
)

const nullSeqStr = "\\N"

// csvSingleEmptyField - csv.Writer writes a blank line for one empty field and csv.Reader skips blank lines
var csvSingleEmptyField = []byte("\"\"\n")

// writeCsvRecord writes the record, quoting a single empty field so the reader does not skip it
func writeCsvRecord(cw *csv.Writer, w io.Writer, record []string) error {
	if len(record) == 1 && record[0] == "" {
		cw.Flush()
		if err := cw.Error(); err != nil {
			return err
		}
		_, err := w.Write(csvSingleEmptyField)
		return err
	}
	if err := cw.Write(record); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

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
	if idx >= rr.size || idx < 0 {
		return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	pos := idx
	if len(rr.originalIdxToOrdered) > 0 {
		var ok bool
		if pos, ok = rr.originalIdxToOrdered[idx]; !ok {
			return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
		}
	}
	if pos >= len(rr.Data) {
		return nil, fmt.Errorf("attribute with idx=%d is not found: record has %d fields", idx, len(rr.Data))
	}

	val := rr.Data[pos]

	if val == nullSeqStr {
		return NewRawValue(nil, true), nil
	}
	return NewRawValue([]byte(val), false), nil
}

func (rr *RawRecordCsv) SetColumn(idx int, v *RawValue) error {
	if idx >= rr.size || idx < 0 {
		return fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	pos := idx
	if len(rr.originalIdxToOrdered) > 0 {
		var ok bool
		if pos, ok = rr.originalIdxToOrdered[idx]; !ok {
			return fmt.Errorf("attribute with idx=%d is not found", idx)
		}
	}
	if pos >= len(rr.Data) {
		return fmt.Errorf("attribute with idx=%d is not found: record has %d fields", idx, len(rr.Data))
	}

	if v.IsNull {
		rr.Data[pos] = nullSeqStr
	} else {
		rr.Data[pos] = string(v.Data)
	}
	return nil
}

func (rr *RawRecordCsv) Encode() ([]byte, error) {
	var err error
	if err = writeCsvRecord(rr.w, rr.buf, rr.Data); err != nil {
		return nil, fmt.Errorf("error writing to buf: %w", err)
	}
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
