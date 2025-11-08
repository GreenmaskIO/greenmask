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
	"fmt"

	"github.com/stretchr/testify/mock"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	_ commonininterfaces.TableDriver = (*TableDriverMock)(nil)
)

type TableDriverMock struct {
	mock.Mock
}

func NewTableDriverMock() *TableDriverMock {
	return &TableDriverMock{}
}

func (t *TableDriverMock) GetColumnIdxByName(name string) (int, error) {
	args := t.Called(name)
	return args.Int(0), args.Error(1)
}

func (t *TableDriverMock) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	args := t.Called(name, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *TableDriverMock) EncodeValueByTypeOid(oid commonmodels.VirtualOID, src any, buf []byte) ([]byte, error) {
	args := t.Called(oid, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *TableDriverMock) DecodeValueByTypeName(name string, src []byte) (any, error) {
	args := t.Called(name, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0), args.Error(1)
}

func (t *TableDriverMock) DecodeValueByTypeOid(oid commonmodels.VirtualOID, src []byte) (any, error) {
	args := t.Called(oid, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0), args.Error(1)
}

func (t *TableDriverMock) ScanValueByTypeName(name string, src []byte, dest any) error {
	args := t.Called(name, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *TableDriverMock) ScanValueByTypeOid(oid commonmodels.VirtualOID, src []byte, dest any) error {
	args := t.Called(oid, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *TableDriverMock) TypeExistsByName(name string) bool {
	args := t.Called(name)
	if args.Get(0) == nil {
		return false
	}
	exists, ok := args.Get(0).(bool)
	if !ok {
		panic(fmt.Sprintf("expected bool, got %T", args.Get(0)))
	}
	return exists
}

func (t *TableDriverMock) TypeExistsByOid(oid commonmodels.VirtualOID) bool {
	args := t.Called(oid)
	if args.Get(0) == nil {
		return false
	}
	exists, ok := args.Get(0).(bool)
	if !ok {
		panic(fmt.Sprintf("expected bool, got %T", args.Get(0)))
	}
	return exists
}

func (t *TableDriverMock) GetTypeOid(name string) (commonmodels.VirtualOID, error) {
	args := t.Called(name)
	if args.Get(0) == nil {
		return commonmodels.VirtualOID(0), args.Error(1)
	}
	oid, ok := args.Get(0).(commonmodels.VirtualOID)
	if !ok {
		panic(fmt.Sprintf("expected commonmodels.VirtualOID, got %T", args.Get(0)))
	}
	return oid, args.Error(1)
}

func (t *TableDriverMock) GetCanonicalTypeName(typeName string, typeOid commonmodels.VirtualOID) (string, error) {
	args := t.Called(typeName, typeOid)
	if args.Get(0) == nil {
		return "", args.Error(1)
	}
	canonicalTypeName, ok := args.Get(0).(string)
	if !ok {
		panic(fmt.Sprintf("expected string, got %T", args.Get(0)))
	}
	return canonicalTypeName, args.Error(1)
}

func (t *TableDriverMock) EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error) {
	args := t.Called(idx, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *TableDriverMock) EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error) {
	args := t.Called(name, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *TableDriverMock) ScanValueByColumnIdx(idx int, src []byte, dest any) error {
	args := t.Called(idx, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *TableDriverMock) ScanValueByColumnName(name string, src []byte, dest any) error {
	args := t.Called(name, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *TableDriverMock) DecodeValueByColumnIdx(idx int, src []byte) (any, error) {
	args := t.Called(idx, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	value, ok := args.Get(0).(any)
	if !ok {
		panic(fmt.Sprintf("expected any, got %T", args.Get(0)))
	}
	return value, args.Error(1)
}

func (t *TableDriverMock) DecodeValueByColumnName(name string, src []byte) (any, error) {
	args := t.Called(name, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	value, ok := args.Get(0).(any)
	if !ok {
		panic(fmt.Sprintf("expected any, got %T", args.Get(0)))
	}
	return value, args.Error(1)
}

func (t *TableDriverMock) GetColumnByName(name string) (*commonmodels.Column, error) {
	args := t.Called(name)
	return args.Get(0).(*commonmodels.Column), args.Error(1)
}

func (t *TableDriverMock) Table() *commonmodels.Table {
	args := t.Called()
	if args.Get(0) == nil {
		return nil
	}
	table, ok := args.Get(0).(*commonmodels.Table)
	if !ok {
		panic(fmt.Sprintf("expected *commonmodels.Table, got %T", args.Get(0)))
	}
	return table
}

func (t *TableDriverMock) GetCanonicalTypeClassName(typeName string, typeOid commonmodels.VirtualOID) (commonmodels.TypeClass, error) {
	args := t.Called(typeName, typeOid)
	if args.Get(0) == nil {
		return "", args.Error(1)
	}
	canonicalTypeClass, ok := args.Get(0).(commonmodels.TypeClass)
	if !ok {
		panic(fmt.Sprintf("expected commonmodels.TypeClass, got %T", args.Get(0)))
	}
	return canonicalTypeClass, args.Error(1)
}
