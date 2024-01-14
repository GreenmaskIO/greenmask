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

// RawRecordStr - record data transfer object for interaction with custom transformer via PIPE
type RawRecordStr map[int]*RawValueStr

func (rrs *RawRecordStr) GetColumn(idx int) (*RawValue, error) {
	res, ok := (*rrs)[idx]
	if !ok {
		return nil, fmt.Errorf("column with idx=%d is not found", idx)
	}
	var data []byte
	if res.Data != nil {
		data = []byte(*res.Data)
	}
	return NewRawValue(data, res.IsNull), nil
}

func (rrs *RawRecordStr) SetColumn(idx int, v *RawValue) error {
	(*rrs)[idx] = NewRawValueStr(v.Data, v.IsNull)
	return nil
}

func (rrs *RawRecordStr) Encode() ([]byte, error) {
	res, err := json.Marshal(rrs)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %w", err)
	}
	return res, nil
}

func (rrs *RawRecordStr) Decode(data []byte) error {
	*rrs = make(map[int]*RawValueStr, len(*rrs))
	return json.Unmarshal(data, *rrs)
}

func (rrs *RawRecordStr) Length() int {
	return len(*rrs)
}

func (rrs *RawRecordStr) Clean() {
	for key := range *rrs {
		delete(*rrs, key)
	}
}
