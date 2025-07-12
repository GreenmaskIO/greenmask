package mocks

import (
	"github.com/stretchr/testify/mock"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	_ commonininterfaces.DBMSDriver = (*DBMSDriverMock)(nil)
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

func (m *DBMSDriverMock) EncodeValueByTypeOid(oid commonmodels.VirtualOID, src any, buf []byte) ([]byte, error) {
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

func (m *DBMSDriverMock) DecodeValueByTypeOid(oid commonmodels.VirtualOID, src []byte) (any, error) {
	args := m.Called(oid, src)
	return args.Get(0), args.Error(1)
}

func (m *DBMSDriverMock) ScanValueByTypeName(name string, src []byte, dest any) error {
	args := m.Called(name, src, dest)
	return args.Error(0)
}

func (m *DBMSDriverMock) ScanValueByTypeOid(oid commonmodels.VirtualOID, src []byte, dest any) error {
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

func (m *DBMSDriverMock) TypeExistsByOid(oid commonmodels.VirtualOID) bool {
	args := m.Called(oid)
	return args.Bool(0)
}

func (m *DBMSDriverMock) GetTypeOid(name string) (commonmodels.VirtualOID, error) {
	args := m.Called(name)
	return args.Get(0).(commonmodels.VirtualOID), args.Error(1)
}

func (m *DBMSDriverMock) GetCanonicalTypeName(typeName string, typeOid commonmodels.VirtualOID) (string, error) {
	args := m.Called(typeName, typeOid)
	return args.String(0), args.Error(1)
}
