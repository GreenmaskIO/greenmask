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

var DefaultNullSeq RawRecordText = []byte("\\N")

type RawRecordText []byte

func NewRawRecordText() *RawRecordText {
	return new(RawRecordText)
}

func (r *RawRecordText) GetColumn(idx int) (*RawValue, error) {
	if r == &DefaultNullSeq {
		return NewRawValue(nil, true), nil
	}
	return NewRawValue(*r, false), nil
}

func (r *RawRecordText) SetColumn(idx int, v *RawValue) error {
	if v.IsNull {
		*r = DefaultNullSeq
		return nil
	}
	*r = v.Data
	return nil
}

func (r *RawRecordText) Encode() ([]byte, error) {
	return *r, nil
}

func (r *RawRecordText) Decode(data []byte) error {
	*r = data
	return nil
}

func (r *RawRecordText) Length() int {
	return 1
}

func (r *RawRecordText) Clean() {
	*r = (*r)[:0]
}
