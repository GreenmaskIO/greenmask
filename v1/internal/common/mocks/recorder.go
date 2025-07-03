package mocks

import (
	"github.com/stretchr/testify/mock"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type RecorderMock struct {
	mock.Mock
}

func NewRecorderMock() *RecorderMock {
	return &RecorderMock{}
}

func (r *RecorderMock) GetRawColumnValueByIdx(columnIdx int) (*commonmodels.ColumnRawValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnRawValue), nil
}

func (r *RecorderMock) GetColumnValueByIdx(columnIdx int) (*commonmodels.ColumnValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnValue), nil
}

func (r *RecorderMock) GetColumnValueByName(columnName string) (*commonmodels.ColumnValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnValue), nil
}

func (r *RecorderMock) GetRawColumnValueByName(columnName string) (*commonmodels.ColumnRawValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnRawValue), nil
}

func (r *RecorderMock) SetColumnValueByIdx(columnIdx int, v any) error {
	args := r.Called(columnIdx, v)
	return args.Error(0)
}

func (r *RecorderMock) SetRawColumnValueByIdx(columnIdx int, value *commonmodels.ColumnRawValue) error {
	args := r.Called(columnIdx, value)
	return args.Error(0)
}

func (r *RecorderMock) SetColumnValueByName(columnName string, v any) error {
	args := r.Called(columnName, v)
	return args.Error(0)
}

func (r *RecorderMock) SetRawColumnValueByName(columnName string, value *commonmodels.ColumnRawValue) error {
	args := r.Called(columnName, value)
	return args.Error(0)
}

func (r *RecorderMock) GetColumnByName(columnName string) (*commonmodels.Column, bool) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, false
	}
	return args.Get(0).(*commonmodels.Column), true
}

func (r *RecorderMock) TableDriver() commonininterfaces.TableDriver {
	args := r.Called()
	return args.Get(0).(commonininterfaces.TableDriver)
}
