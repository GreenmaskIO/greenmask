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
	"bytes"
	"context"
	"io"

	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type StorageMock struct {
	mock.Mock
	// Data - contains a data that was read on PutObject call.
	Data *bytes.Buffer
}

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

func (s *StorageMock) ListDir(ctx context.Context) (files []string, dirs []interfaces.Storager, err error) {
	args := s.Called(ctx)
	return args.Get(0).([]string), args.Get(1).([]interfaces.Storager), args.Error(2)
}

func (s *StorageMock) SubStorage(subPath string, relative bool) interfaces.Storager {
	args := s.Called(subPath, relative)
	return args.Get(0).(interfaces.Storager)
}

func (s *StorageMock) Stat(fileName string) (*models.StorageObjectStat, error) {
	args := s.Called(fileName)
	return args.Get(0).(*models.StorageObjectStat), args.Error(1)
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
