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

type RawValue struct {
	Data   []byte `json:"d"`
	IsNull bool   `json:"n"`
}

func NewRawValue(data []byte, isNull bool) *RawValue {
	return &RawValue{
		Data:   data,
		IsNull: isNull,
	}
}

type Value struct {
	Value  any
	IsNull bool
}

func NewValue(v any, isNull bool) *Value {
	return &Value{
		Value:  v,
		IsNull: isNull,
	}
}

type RawValueStr struct {
	Data   *string `json:"d"`
	IsNull bool    `json:"n"`
}

func NewRawValueStr(data []byte, isNull bool) *RawValueStr {
	res := string(data)
	return &RawValueStr{
		Data:   &res,
		IsNull: isNull,
	}
}
