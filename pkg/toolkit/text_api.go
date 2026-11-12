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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

var errTextApiNewLineInValue = errors.New("text driver cannot send values with new line: use csv or json driver")

type TextApi struct {
	attributeName    string
	columnIdx        int
	w                io.Writer
	r                io.Reader
	lineReader       *bufio.Reader
	skipOriginalData bool
	// record - allocated record for Encode - Decode operations
	record *RawRecordText
	// emptyRecord - allocated static empty object in case wi just send \n
	emptyRecord *RawRecordText
	// encBuf and lineBuf - reusable buffers for writing and reading lines
	encBuf  []byte
	lineBuf []byte
}

func NewTextApi(columnIdx int, skipOriginalData bool) (*TextApi, error) {
	return &TextApi{
		columnIdx:        columnIdx,
		skipOriginalData: skipOriginalData,
		record:           NewRawRecordText(),
		emptyRecord:      NewRawRecordText(),
	}, nil
}

func (ta *TextApi) SetWriter(w io.Writer) {
	ta.w = w
}

func (ta *TextApi) SetReader(r io.Reader) {
	ta.r = r
	ta.lineReader = bufio.NewReader(r)
}

func (ta *TextApi) GetRowDriverFromRecord(r *Record) (RowDriver, error) {
	if ta.skipOriginalData {
		return ta.emptyRecord, nil
	}

	v, err := r.GetRawColumnValueByIdx(ta.columnIdx)
	if err != nil {
		return nil, fmt.Errorf("error getting raw attribute: %w", err)
	}
	if err := ta.record.SetColumn(ta.columnIdx, v); err != nil {
		return nil, fmt.Errorf("error setting column value to record: %w", err)
	}
	return ta.record, nil
}

func (ta *TextApi) SetRowDriverToRecord(rd RowDriver, r *Record) error {
	v, err := rd.GetColumn(ta.columnIdx)
	if err != nil {
		return fmt.Errorf(`error getting column "%s" value: %w`, ta.attributeName, err)
	}
	err = r.SetRawColumnValueByIdx(ta.columnIdx, v)
	if err != nil {
		return fmt.Errorf(`error setting column "%s" value to record: %w`, ta.attributeName, err)
	}
	return nil
}

func (ta *TextApi) Encode(ctx context.Context, row RowDriver) (err error) {
	data, err := row.Encode()
	if err != nil {
		return fmt.Errorf("error encodig row data via text interaction API: %w", err)
	}
	// the protocol is line based, so a value with a new line would desync the transformer
	if bytes.IndexByte(data, '\n') != -1 {
		return errTextApiNewLineInValue
	}
	// copy into own buffer: appending to data may overwrite the shared row memory
	ta.encBuf = append(append(ta.encBuf[:0], data...), '\n')
	_, err = ta.w.Write(ta.encBuf)

	if err != nil {
		return err
	}

	return nil
}

func (ta *TextApi) Decode(ctx context.Context) (RowDriver, error) {
	// collect the whole line: ReadLine returns long lines in chunks
	ta.lineBuf = ta.lineBuf[:0]
	for {
		line, isPrefix, err := ta.lineReader.ReadLine()
		if err != nil {
			return nil, err
		}
		ta.lineBuf = append(ta.lineBuf, line...)
		if !isPrefix {
			break
		}
	}

	if err := ta.record.Decode(ta.lineBuf); err != nil {
		return nil, fmt.Errorf("error decoding via text interaction API: %w", err)
	}

	return ta.record, nil
}

func (ta *TextApi) Clean() {
	ta.record.Clean()
}
