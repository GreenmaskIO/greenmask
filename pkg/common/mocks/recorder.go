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

package mocks

import (
	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/stretchr/testify/mock"
)

var (
	_ interfaces.Recorder = (*RecorderMock)(nil)
)

type RecorderMock struct {
	mock.Mock
}

func (r *RecorderMock) SetRow(rawRecord [][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (r *RecorderMock) GetRow() [][]byte {
	//TODO implement me
	panic("implement me")
}

func (r *RecorderMock) ScanColumnValueByIdx(idx int, v any) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RecorderMock) ScanColumnValueByName(name string, v any) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RecorderMock) GetColumnByName(columnName string) (*models.Column, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Column), nil
}

func NewRecorderMock() *RecorderMock {
	return &RecorderMock{}
}

func (r *RecorderMock) IsNullByColumnName(columName string) (bool, error) {
	args := r.Called(columName)
	return args.Bool(0), args.Error(1)
}

func (r *RecorderMock) IsNullByColumnIdx(columIdx int) (bool, error) {
	args := r.Called(columIdx)
	return args.Bool(0), args.Error(1)
}

func (r *RecorderMock) GetRawColumnValueByIdx(columnIdx int) (*models.ColumnRawValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ColumnRawValue), nil
}

func (r *RecorderMock) GetColumnValueByIdx(columnIdx int) (*models.ColumnValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ColumnValue), nil
}

func (r *RecorderMock) GetColumnValueByName(columnName string) (*models.ColumnValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ColumnValue), nil
}

func (r *RecorderMock) GetRawColumnValueByName(columnName string) (*models.ColumnRawValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ColumnRawValue), nil
}

func (r *RecorderMock) SetColumnValueByIdx(columnIdx int, v any) error {
	args := r.Called(columnIdx, v)
	return args.Error(0)
}

func (r *RecorderMock) SetRawColumnValueByIdx(columnIdx int, value *models.ColumnRawValue) error {
	args := r.Called(columnIdx, value)
	return args.Error(0)
}

func (r *RecorderMock) SetColumnValueByName(columnName string, v any) error {
	args := r.Called(columnName, v)
	return args.Error(0)
}

func (r *RecorderMock) SetRawColumnValueByName(columnName string, value *models.ColumnRawValue) error {
	args := r.Called(columnName, value)
	return args.Error(0)
}

func (r *RecorderMock) TableDriver() interfaces.TableDriver {
	args := r.Called()
	return args.Get(0).(interfaces.TableDriver)
}
