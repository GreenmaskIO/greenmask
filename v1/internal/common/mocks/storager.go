package mocks

import (
	"bytes"
	"context"
	"io"

	"github.com/stretchr/testify/mock"

	storages2 "github.com/greenmaskio/greenmask/v1/internal/storages"
)

type StorageMock struct {
	mock.Mock
	// Data - contains a data that was read on PutObject call.
	Data *bytes.Buffer
}

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

func (s *StorageMock) ListDir(ctx context.Context) (files []string, dirs []storages2.Storager, err error) {
	args := s.Called(ctx)
	return args.Get(0).([]string), args.Get(1).([]storages2.Storager), args.Error(2)
}

func (s *StorageMock) SubStorage(subPath string, relative bool) storages2.Storager {
	args := s.Called(subPath, relative)
	return args.Get(0).(storages2.Storager)
}

func (s *StorageMock) Stat(fileName string) (*storages2.ObjectStat, error) {
	args := s.Called(fileName)
	return args.Get(0).(*storages2.ObjectStat), args.Error(1)
}

func (s *StorageMock) GetCwd() string {
	args := s.Called()
	return args.String(0)
}

func (s *StorageMock) Dirname() string {
	args := s.Called()
	return args.String(0)
}

func (s *StorageMock) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	args := s.Called(ctx, filePath)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (s *StorageMock) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	s.Data = bytes.NewBuffer(nil)
	args := s.Called(ctx, filePath, body)
	_, err := io.Copy(s.Data, body)
	if err != nil {
		return err
	}
	return args.Error(0)
}

func (s *StorageMock) Delete(ctx context.Context, filePaths ...string) error {
	args := s.Called(ctx, filePaths)
	return args.Error(0)
}

func (s *StorageMock) DeleteAll(ctx context.Context, pathPrefix string) error {
	args := s.Called(ctx, pathPrefix)
	return args.Error(0)
}

func (s *StorageMock) Exists(ctx context.Context, fileName string) (bool, error) {
	args := s.Called(ctx, fileName)
	return args.Bool(0), args.Error(1)
}
