// Copyright 2025 Greenmask
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

package transformers

import (
	"github.com/tidwall/gjson"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
)

type JsonContext struct {
	exists        bool
	originalValue any
	path          string
	rc            *template.RecordContextReadOnly
}

func NewJsonContext() *JsonContext {
	return &JsonContext{
		rc: template.NewRecordContextReadOnly(),
	}
}

func (jc *JsonContext) setValue(data []byte, path string) {
	res := gjson.GetBytes(data, path)
	jc.originalValue = res.Value()
	jc.exists = res.Exists()
	jc.path = path
}

func (jc *JsonContext) setRecord(r interfaces.Recorder) {
	jc.rc.SetRecord(r)
}

func (jc *JsonContext) GetPath() string {
	return jc.path
}

func (jc *JsonContext) GetOriginalValue() any {
	return jc.originalValue
}

func (jc *JsonContext) OriginalValueExists() bool {
	return jc.exists
}

func (jc *JsonContext) GetColumnValue(name string) (any, error) {
	return jc.rc.GetColumnValue(name)
}

func (jc *JsonContext) GetRawColumnValue(name string) (any, error) {
	return jc.rc.GetRawColumnValue(name)
}

func (jc *JsonContext) EncodeValueByColumn(name string, v any) (any, error) {
	return jc.rc.EncodeValueByColumn(name, v)
}

func (jc *JsonContext) DecodeValueByColumn(name string, v any) (any, error) {
	return jc.rc.DecodeValueByColumn(name, v)
}

func (jc *JsonContext) EncodeValueByType(name string, v any) (any, error) {
	return jc.rc.EncodeValueByType(name, v)
}

func (jc *JsonContext) DecodeValueByType(name string, v any) (any, error) {
	return jc.rc.DecodeValueByType(name, v)
}
