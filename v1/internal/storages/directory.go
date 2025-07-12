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

package storages

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	errPathIsRequired = errors.New("path is required")
)

const (
	directoryStorageDirMode  os.FileMode = 0750
	directoryStorageFileMode os.FileMode = 0650
)

type DirectoryStorage struct {
	dirMode  os.FileMode
	fileMode os.FileMode
	cwd      string
	mx       sync.Mutex
}

func NewDirectoryStorage(cfg DirectoryConfig) (*DirectoryStorage, error) {
	if cfg.Path == "" {
		return nil, errPathIsRequired
	}
	fileInfo, err := os.Stat(cfg.Path)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, errors.New("received directory path is file")
	}
	return &DirectoryStorage{
		dirMode:  directoryStorageDirMode,
		fileMode: directoryStorageFileMode,
		cwd:      cfg.Path,
	}, nil
}

func (s *DirectoryStorage) GetCwd() string {
	return s.cwd
}

func (s *DirectoryStorage) Dirname() string {
	return filepath.Base(s.cwd)
}

func (s *DirectoryStorage) ListDir(ctx context.Context) (files []string, dirs []Storager, err error) {
	entries, err := os.ReadDir(s.cwd)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(
				dirs, &DirectoryStorage{
					cwd:      path.Join(s.cwd, entry.Name()),
					dirMode:  s.dirMode,
					fileMode: s.fileMode,
				},
			)
		} else {
			files = append(files, entry.Name())
		}
	}
	return
}

func (s *DirectoryStorage) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	reader, err = os.Open(path.Join(s.cwd, filePath))
	return
}

func (s *DirectoryStorage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	_, err := os.Stat(path.Join(s.cwd, path.Dir(filePath)))
	var errNo syscall.Errno
	if err != nil && errors.As(err, &errNo) && errNo == 0x2 {
		s.mx.Lock()
		if err = os.MkdirAll(path.Join(s.cwd, path.Dir(filePath)), s.dirMode); err != nil {
			s.mx.Unlock()
			return fmt.Errorf("error creating directory: %w", err)
		}
		s.mx.Unlock()
	} else if err != nil {
		return fmt.Errorf("error getting file stat: %w", err)
	}
	f, err := os.Create(path.Join(s.cwd, filePath))
	if err != nil {
		return fmt.Errorf("unable to create file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	done := make(chan struct{})
	go func() {
		_, err = io.Copy(f, body)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	if err != nil {
		return fmt.Errorf("error writing data: %w", err)
	}
	return f.Close()
}

func (s *DirectoryStorage) Delete(ctx context.Context, filePaths ...string) error {
	for _, fp := range filePaths {
		fileInfo, err := os.Stat(path.Join(s.cwd, fp))
		if err != nil {
			return err
		}
		if fileInfo.IsDir() {
			err = os.RemoveAll(path.Join(s.cwd, fp))
			if err != nil {
				return fmt.Errorf(`error deliting directory %s: %w`, fp, err)
			}
		} else {
			err = os.Remove(path.Join(s.cwd, fp))
			if err != nil {
				return fmt.Errorf(`error deliting file %s: %w`, fp, err)
			}
		}
	}
	return nil
}

func (s *DirectoryStorage) DeleteAll(ctx context.Context, pathPrefix string) error {
	fileInfo, err := os.Stat(path.Join(s.cwd, pathPrefix))
	if err != nil {
		return err
	}
	if fileInfo.IsDir() {
		err = os.RemoveAll(path.Join(s.cwd, pathPrefix))
		if err != nil {
			return fmt.Errorf(`error deliting directory %s: %w`, pathPrefix, err)
		}
	} else {
		err = os.Remove(path.Join(s.cwd, pathPrefix))
		if err != nil {
			return fmt.Errorf(`error deliting file %s: %w`, pathPrefix, err)
		}
	}
	return nil
}

func (s *DirectoryStorage) Exists(ctx context.Context, fileName string) (bool, error) {
	_, err := os.Stat(path.Join(s.cwd, fileName))
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *DirectoryStorage) SubStorage(dp string, relative bool) Storager {
	dirPath := dp
	if relative {
		dirPath = path.Join(s.cwd, dp)
	}
	return &DirectoryStorage{
		cwd:      dirPath,
		dirMode:  s.dirMode,
		fileMode: s.fileMode,
	}
}

func (s *DirectoryStorage) Stat(fileName string) (*ObjectStat, error) {
	fullPath := path.Join(s.cwd, fileName)
	fileInfo, err := os.Stat(fullPath)
	var errNo syscall.Errno
	if err != nil && errors.As(err, &errNo) && errNo == 0x2 {
		return &ObjectStat{
			Name:         fullPath,
			LastModified: time.Time{},
			Exist:        false,
		}, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting file stat: %w", err)
	}

	return &ObjectStat{
		Name:         fullPath,
		LastModified: fileInfo.ModTime(),
		Exist:        true,
	}, nil
}
