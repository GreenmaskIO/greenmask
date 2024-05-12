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

package transformers

import (
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type ColumnContext struct {
	columnType string
	columnName string
	rc         *toolkit.RecordContext
}

func NewColumnContext(columnType string, columnName string) *ColumnContext {
	return &ColumnContext{
		columnType: columnType,
		columnName: columnName,
		rc:         &toolkit.RecordContext{},
	}
}

func (cc *ColumnContext) clean() {
	cc.rc.Clean()
}

func (cc *ColumnContext) setRecord(r *toolkit.Record) {
	cc.rc.SetRecord(r)
}

func (cc *ColumnContext) GetColumnType() string {
	return cc.columnType
}

func (cc *ColumnContext) GetValue() (any, error) {
	return cc.rc.GetColumnValue(cc.columnName)
}

func (cc *ColumnContext) GetRawValue() (any, error) {
	return cc.rc.GetRawColumnValue(cc.columnName)
}

func (cc *ColumnContext) GetColumnValue(name string) (any, error) {
	return cc.rc.GetColumnValue(name)
}

func (cc *ColumnContext) GetColumnRawValue(name string) (any, error) {
	return cc.rc.GetRawColumnValue(name)
}

func (cc *ColumnContext) EncodeValue(v any) (any, error) {
	return cc.rc.EncodeValueByColumn(cc.columnName, v)
}

func (cc *ColumnContext) DecodeValue(v any) (any, error) {
	return cc.rc.DecodeValueByColumn(cc.columnType, v)
}

func (cc *ColumnContext) EncodeValueByColumn(name string, v any) (any, error) {
	return cc.rc.EncodeValueByColumn(name, v)
}

func (cc *ColumnContext) DecodeValueByColumn(name string, v any) (any, error) {
	return cc.rc.DecodeValueByColumn(name, v)
}

func (cc *ColumnContext) EncodeValueByType(name string, v any) (any, error) {
	return cc.rc.EncodeValueByType(name, v)
}

func (cc *ColumnContext) DecodeValueByType(name string, v any) (any, error) {
	return cc.rc.DecodeValueByType(name, v)
}
