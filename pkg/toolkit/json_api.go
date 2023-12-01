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
	"context"
	"fmt"
	"io"

	jsoniter "github.com/json-iterator/go"
)

const (
	JsonBytesFormatName = "bytes"
	JsonTextFormatName  = "text"
)

var json = jsoniter.ConfigDefault
var emptyJson = []byte("{}\n")

type JsonApi struct {
	transferringColumns []int
	affectedColumns     []int
	tupleLength         int
	w                   io.Writer
	r                   io.Reader
	encoder             *jsoniter.Encoder
	record              RowDriver
	reader              *bufio.Reader
}

func NewJsonApi(
	transferringColumns []int, affectedColumns []int, format string,
) (*JsonApi, error) {

	var record RowDriver

	switch format {
	case JsonBytesFormatName:
		r := make(RawRecord, len(transferringColumns))
		record = &r
	case JsonTextFormatName:
		r := make(RawRecordStr, len(transferringColumns))
		record = &r
	default:
		return nil, fmt.Errorf(`unknown format "%s"`, format)
	}

	return &JsonApi{
		transferringColumns: transferringColumns,
		affectedColumns:     affectedColumns,
		tupleLength:         len(transferringColumns),
		record:              record,
	}, nil
}

func (j *JsonApi) SetWriter(w io.Writer) {
	j.w = w
	j.encoder = json.NewEncoder(w)
}

func (j *JsonApi) SetReader(r io.Reader) {
	j.r = r
	j.reader = bufio.NewReader(r)
}

func (j *JsonApi) GetRowDriverFromRecord(r *Record) (RowDriver, error) {
	for _, columnIdx := range j.transferringColumns {

		v, err := r.GetRawAttributeValueByIdx(columnIdx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		if err = j.record.SetColumn(columnIdx, v); err != nil {
			return nil, fmt.Errorf("unable to set new value: %w", err)
		}
	}
	return j.record, nil
}

func (j *JsonApi) SetRowDriverToRecord(rd RowDriver, r *Record) error {
	for _, columnIdx := range j.affectedColumns {
		v, err := rd.GetColumn(columnIdx)
		if err != nil {
			return fmt.Errorf(`error getting column %d value: %w`, columnIdx, err)
		}
		err = r.SetRawAttributeValueByIdx(columnIdx, v)
		if err != nil {
			return fmt.Errorf(`error setting column %d value to record: %w`, columnIdx, err)
		}
	}
	return nil
}

func (j *JsonApi) Encode(ctx context.Context, row RowDriver) (err error) {
	if row.Length() == 0 {
		_, err = j.w.Write(emptyJson)
		if err != nil {
			return err
		}
	} else {
		if err = j.encoder.Encode(row); err != nil {
			return err
		}
	}

	return nil
}

func (j *JsonApi) Decode(ctx context.Context) (RowDriver, error) {
	// TODO:  Review those changes later.
	//        This may cause an error if replace Unmarshal with why json.Decode. I don't know the reason but it
	//        unexpectedly failure on custom transformer side. I suspect it is "github.com/json-iterator/go" issue - it might be internal buffer or smth else.
	//        I can significantly increase the interaction, but works

	data, _, err := j.reader.ReadLine()
	if err != nil {
		return nil, fmt.Errorf("error reading line: %w", err)
	}
	if err = json.Unmarshal(data, &j.record); err != nil {
		return nil, fmt.Errorf("error unmarshalling json: %w", err)
	}

	return j.record, nil
}

func (j *JsonApi) Clean() {
	j.record.Clean()
}
