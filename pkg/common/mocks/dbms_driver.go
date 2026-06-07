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
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/stretchr/testify/mock"
)

var (
	_ core.DBMSDriver = (*DBMSDriverMock)(nil)
)

// DBMSDriverMock is a mock implementation of DBMSDriver for testing using testify/mock
type DBMSDriverMock struct {
	mock.Mock
}

func NewDBMSDriverMock() *DBMSDriverMock {
	return &DBMSDriverMock{}
}

func (m *DBMSDriverMock) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	args := m.Called(name, src, buf)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *DBMSDriverMock) EncodeValueByTypeOid(oid core.VirtualOID, src any, buf []byte) ([]byte, error) {
	args := m.Called(oid, src, buf)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *DBMSDriverMock) DecodeValueByTypeName(name string, src []byte) (any, error) {
	args := m.Called(name, src)
	return args.Get(0), args.Error(1)
}

func (m *DBMSDriverMock) DecodeValueByTypeOid(oid core.VirtualOID, src []byte) (any, error) {
	args := m.Called(oid, src)
	return args.Get(0), args.Error(1)
}

func (m *DBMSDriverMock) ScanValueByTypeName(name string, src []byte, dest any) error {
	args := m.Called(name, src, dest)
	return args.Error(0)
}

func (m *DBMSDriverMock) ScanValueByTypeOid(oid core.VirtualOID, src []byte, dest any) error {
	args := m.Called(oid, src, dest)
	if vv, ok := dest.(*string); ok {
		*vv = string(src)
	} else {
		panic("unable to assert string")
	}
	return args.Error(0)
}

func (m *DBMSDriverMock) TypeExistsByName(name string) bool {
	args := m.Called(name)
	return args.Bool(0)
}

func (m *DBMSDriverMock) TypeExistsByOid(oid core.VirtualOID) bool {
	args := m.Called(oid)
	return args.Bool(0)
}

func (m *DBMSDriverMock) GetTypeOid(name string) (core.VirtualOID, error) {
	args := m.Called(name)
	return args.Get(0).(core.VirtualOID), args.Error(1)
}

func (m *DBMSDriverMock) GetCanonicalTypeName(typeName string, typeOid core.VirtualOID) (string, error) {
	args := m.Called(typeName, typeOid)
	return args.String(0), args.Error(1)
}

func (m *DBMSDriverMock) GetCanonicalTypeClassName(typeName string, typeOid core.VirtualOID) (core.TypeClass, error) {
	args := m.Called(typeName, typeOid)
	return args.Get(0).(core.TypeClass), args.Error(1)
}
