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
	"context"
	"encoding/csv"
	"fmt"
	"io"
)

type CsvApi struct {
	transferringColumns []int
	affectedColumns     []int
	w                   *csv.Writer
	r                   *csv.Reader
	record              *RawRecordCsv
}

func NewCsvApi(transferringColumns []int, affectedColumns []int, size int) *CsvApi {
	return &CsvApi{
		transferringColumns: transferringColumns,
		affectedColumns:     affectedColumns,
		record:              NewRawRecordCsv(size),
	}
}

func (ca *CsvApi) SetWriter(w io.Writer) {
	ca.w = csv.NewWriter(w)
}

func (ca *CsvApi) SetReader(r io.Reader) {
	ca.r = csv.NewReader(r)
}

func (ca *CsvApi) GetRowDriverFromRecord(r *Record) (RowDriver, error) {
	for _, columnIdx := range ca.transferringColumns {

		v, err := r.GetRawColumnValueByIdx(columnIdx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		if err = ca.record.SetColumn(columnIdx, v); err != nil {
			return nil, fmt.Errorf("unable to set new value: %w", err)
		}
	}
	return ca.record, nil
}

func (ca *CsvApi) SetRowDriverToRecord(rd RowDriver, r *Record) error {
	for _, columnIdx := range ca.affectedColumns {
		v, err := rd.GetColumn(columnIdx)
		if err != nil {
			return fmt.Errorf(`error getting column %d value: %w`, columnIdx, err)
		}
		err = r.SetRawColumnValueByIdx(columnIdx, v)
		if err != nil {
			return fmt.Errorf(`error setting column %d value to record: %w`, columnIdx, err)
		}
	}
	return nil
}

func (ca *CsvApi) Encode(ctx context.Context, row RowDriver) (err error) {
	csvRow, ok := row.(*RawRecordCsv)
	if !ok {
		return fmt.Errorf("expected RawRecordCsv but received another driver: %w", err)
	}

	err = ca.w.Write(csvRow.Data)
	if err != nil {
		return fmt.Errorf("error ecnoding row: %w", err)
	}
	ca.w.Flush()
	if ca.record != nil {
		ca.record.Clean()
	}
	return nil
}

func (ca *CsvApi) Decode(ctx context.Context) (RowDriver, error) {
	ca.record.Clean()
	data, err := ca.r.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading csv record: %w", err)
	}
	ca.record.Data = data

	return ca.record, nil
}

func (ca *CsvApi) Clean() {
	ca.record.Clean()
}
