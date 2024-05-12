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

type RawRecord map[int]*RawValue

func (rr *RawRecord) GetColumn(idx int) (*RawValue, error) {
	res, ok := (*rr)[idx]
	if !ok {
		return nil, fmt.Errorf("column with idx=%d is not found", idx)
	}
	return res, nil
}

func (rr *RawRecord) SetColumn(idx int, v *RawValue) error {
	(*rr)[idx] = v
	return nil
}

func (rr *RawRecord) Encode() ([]byte, error) {
	res, err := json.Marshal(rr)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %w", err)
	}
	return res, nil
}

func (rr *RawRecord) Decode(data []byte) error {
	*rr = make(map[int]*RawValue, len(*rr))
	return json.Unmarshal(data, rr)
}

func (rr *RawRecord) Length() int {
	return len(*rr)
}

func (rr *RawRecord) Clean() {
	for key := range *rr {
		delete(*rr, key)
	}
}
