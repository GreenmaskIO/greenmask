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

package interfaces

import (
	models2 "github.com/greenmaskio/greenmask/pkg/common/models"
)

type Recorder interface {
	SetRow(rawRecord [][]byte) error
	GetRow() [][]byte
	IsNullByColumnName(columName string) (bool, error)
	IsNullByColumnIdx(columIdx int) (bool, error)
	ScanColumnValueByIdx(idx int, v any) (bool, error)
	ScanColumnValueByName(name string, v any) (bool, error)
	GetRawColumnValueByIdx(columnIdx int) (*models2.ColumnRawValue, error)
	GetColumnValueByIdx(columnIdx int) (*models2.ColumnValue, error)
	GetColumnValueByName(columnName string) (*models2.ColumnValue, error)
	GetRawColumnValueByName(columnName string) (*models2.ColumnRawValue, error)
	SetColumnValueByIdx(columnIdx int, v any) error
	SetRawColumnValueByIdx(columnIdx int, value *models2.ColumnRawValue) error
	SetColumnValueByName(columnName string, v any) error
	SetRawColumnValueByName(columnName string, value *models2.ColumnRawValue) error
	GetColumnByName(columnName string) (*models2.Column, error)
	TableDriver() TableDriver
}
