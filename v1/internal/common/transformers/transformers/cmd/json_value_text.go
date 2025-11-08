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

import "github.com/greenmaskio/greenmask/v1/internal/common/utils"

type JsonAttrRawValueText struct {
	Data   *string `json:"d"`
	IsNull bool    `json:"n"`
}

func NewJsonAttrRawValueText() *JsonAttrRawValueText {
	return &JsonAttrRawValueText{}
}

func (j *JsonAttrRawValueText) GetData() []byte {
	if j.Data == nil {
		return nil
	}
	return []byte(*j.Data)
}

func (j *JsonAttrRawValueText) IsValueNull() bool {
	return j.IsNull
}

func (j *JsonAttrRawValueText) SetData(bytes []byte) {
	if j.Data == nil {
		j.Data = nil
	}
	j.Data = utils.New(string(bytes))
}

func (j *JsonAttrRawValueText) SetNull(b bool) {
	j.IsNull = b
}
func (j *JsonAttrRawValueText) Json() {}
