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

package cmd

type JsonAttrRawValueBytes struct {
	Data   []byte `json:"d"`
	IsNull bool   `json:"n"`
}

func NewJsonAttrRawValueBytes() *JsonAttrRawValueBytes {
	return &JsonAttrRawValueBytes{}
}

func (j *JsonAttrRawValueBytes) GetData() []byte {
	return j.Data
}

func (j *JsonAttrRawValueBytes) IsValueNull() bool {
	return j.IsNull
}

func (j *JsonAttrRawValueBytes) SetData(bytes []byte) {
	j.Data = bytes
}

func (j *JsonAttrRawValueBytes) SetNull(b bool) {
	j.IsNull = b
}

func (j *JsonAttrRawValueBytes) Json() {}
