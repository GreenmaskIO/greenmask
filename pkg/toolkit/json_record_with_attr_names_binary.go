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
	"encoding/json"
	"fmt"
)

type RawRecordAttrsText map[string]*RawValueStr

type JsonRecordWithAttrNames struct {
	idxToNames map[int]string
	record     RawRecordAttrsText
}

func NewJsonRecordWithAttrNamesText(columns []*Column) *JsonRecordWithAttrNames {
	idxToNames := make(map[int]string, len(columns))

	for _, c := range columns {
		idxToNames[c.Idx] = c.Name
	}

	return &JsonRecordWithAttrNames{
		idxToNames: idxToNames,
		record:     make(RawRecordAttrsText, len(columns)),
	}
}

func (rr *JsonRecordWithAttrNames) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &rr.record)
}

func (rr *JsonRecordWithAttrNames) MarshalJSON() ([]byte, error) {
	return json.Marshal(rr.record)
}

func (rr *JsonRecordWithAttrNames) GetColumn(idx int) (*RawValue, error) {
	name, ok := rr.idxToNames[idx]
	if !ok {
		return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	res, ok := rr.record[name]
	if !ok {
		return nil, fmt.Errorf("attribute with name=%s is not found", name)
	}
	return &RawValue{Data: []byte(*res.Data), IsNull: res.IsNull}, nil
}

func (rr *JsonRecordWithAttrNames) SetColumn(idx int, v *RawValue) error {
	name, ok := rr.idxToNames[idx]
	if !ok {
		return fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	data := string(v.Data)
	rr.record[name] = &RawValueStr{Data: &data, IsNull: v.IsNull}
	return nil
}

func (rr *JsonRecordWithAttrNames) Encode() ([]byte, error) {
	res, err := json.Marshal(rr.record)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %w", err)
	}
	return res, nil
}

func (rr *JsonRecordWithAttrNames) Decode(data []byte) error {
	record := make(RawRecordAttrsText, len(rr.idxToNames))
	return json.Unmarshal(data, &record)
}

func (rr *JsonRecordWithAttrNames) Length() int {
	return len(rr.record)
}

func (rr *JsonRecordWithAttrNames) Clean() {
	for key := range rr.record {
		delete(rr.record, key)
	}
}
