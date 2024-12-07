package testutils

import (
	"context"
	"io"

	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/domains"
)

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) GetCwd() string {
	args := s.Called()
	return args.String(0)
}

func (s *StorageMock) Dirname() string {
	args := s.Called()
	return args.String(0)
}

func (s *StorageMock) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {
	args := s.Called(ctx)
	return args.Get(0).([]string), args.Get(1).([]storages.Storager), args.Error(2)
}

func (s *StorageMock) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	args := s.Called(ctx, filePath)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (s *StorageMock) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	args := s.Called(ctx, filePath, body)
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

func (s *StorageMock) SubStorage(subPath string, relative bool) storages.Storager {
	args := s.Called(subPath, relative)
	return args.Get(0).(storages.Storager)
}

func (s *StorageMock) Stat(fileName string) (*domains.ObjectStat, error) {
	args := s.Called(fileName)
	return args.Get(0).(*domains.ObjectStat), args.Error(1)
}
