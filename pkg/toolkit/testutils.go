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

var testNullSeq = "\\N"
var testDelim byte = '\t'

type TestRowDriver struct {
	row []string
}

func newTestRowDriver(row []string) *TestRowDriver {
	return &TestRowDriver{row: row}
}

func (trd *TestRowDriver) GetColumn(idx int) (*RawValue, error) {
	val := trd.row[idx]
	if val == testNullSeq {
		return NewRawValue(nil, true), nil
	}
	return NewRawValue([]byte(val), false), nil
}

func (trd *TestRowDriver) SetColumn(idx int, v *RawValue) error {
	if v.IsNull {
		trd.row[idx] = testNullSeq
	} else {
		trd.row[idx] = string(v.Data)
	}
	return nil
}

func (trd *TestRowDriver) Encode() ([]byte, error) {
	var res []byte
	for idx, v := range trd.row {
		res = append(res, []byte(v)...)
		if idx != len(trd.row)-1 {
			res = append(res, testDelim)
		}
	}
	return res, nil
}

func (trd *TestRowDriver) Decode([]byte) error {
	panic("is not implemented")
}

func (trd *TestRowDriver) Length() int {
	return len(trd.row)
}

func (trd *TestRowDriver) Clean() {

}
