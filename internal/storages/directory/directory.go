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

package directory

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

	"github.com/greenmaskio/greenmask/internal/storages"
)

const (
	dirMode  os.FileMode = 0750
	fileMode os.FileMode = 0650
)

type Storage struct {
	dirMode  os.FileMode
	fileMode os.FileMode
	cwd      string
	mx       sync.Mutex
}

func NewStorage(cfg Config) (*Storage, error) {
	// TODO: We would replace hardcoded file mask to Umask for unix system
	fileInfo, err := os.Stat(cfg.Path)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, errors.New("received directory path is file")
	}
	return &Storage{
		dirMode:  dirMode,
		fileMode: fileMode,
		cwd:      cfg.Path,
	}, nil
}

func (d *Storage) GetCwd() string {
	return d.cwd
}

func (d *Storage) Dirname() string {
	return filepath.Base(d.cwd)
}

func (d *Storage) ListDir(ctx context.Context) (files []string, dirs []storages.Storager, err error) {
	entries, err := os.ReadDir(d.cwd)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(
				dirs, &Storage{
					cwd:      path.Join(d.cwd, entry.Name()),
					dirMode:  d.dirMode,
					fileMode: d.fileMode,
				},
			)
		} else {
			files = append(files, entry.Name())
		}
	}
	return
}

func (d *Storage) GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	reader, err = os.Open(path.Join(d.cwd, filePath))
	return
}

func (d *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	_, err := os.Stat(path.Join(d.cwd, path.Dir(filePath)))
	var errNo syscall.Errno
	if err != nil && errors.As(err, &errNo) && errNo == 0x2 {
		d.mx.Lock()
		if err = os.MkdirAll(path.Join(d.cwd, path.Dir(filePath)), d.dirMode); err != nil {
			d.mx.Unlock()
			return fmt.Errorf("error creating directory: %w", err)
		}
		d.mx.Unlock()
	} else if err != nil {
		return fmt.Errorf("error getting file stat: %w", err)
	}
	f, err := os.Create(path.Join(d.cwd, filePath))
	if err != nil {
		return fmt.Errorf("unable to create file: %w", err)
	}

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
	return nil
}

func (d *Storage) Delete(ctx context.Context, filePaths ...string) error {
	for _, fp := range filePaths {
		fileInfo, err := os.Stat(path.Join(d.cwd, fp))
		if err != nil {
			return err
		}
		if fileInfo.IsDir() {
			err = os.RemoveAll(path.Join(d.cwd, fp))
			if err != nil {
				return fmt.Errorf(`error deliting directory %s: %w`, fp, err)
			}
		} else {
			err = os.Remove(path.Join(d.cwd, fp))
			if err != nil {
				return fmt.Errorf(`error deliting file %s: %w`, fp, err)
			}
		}
	}
	return nil
}

func (d *Storage) DeleteAll(ctx context.Context, pathPrefix string) error {
	fileInfo, err := os.Stat(path.Join(d.cwd, pathPrefix))
	if err != nil {
		return err
	}
	if fileInfo.IsDir() {
		err = os.RemoveAll(path.Join(d.cwd, pathPrefix))
		if err != nil {
			return fmt.Errorf(`error deliting directory %s: %w`, pathPrefix, err)
		}
	} else {
		err = os.Remove(path.Join(d.cwd, pathPrefix))
		if err != nil {
			return fmt.Errorf(`error deliting file %s: %w`, pathPrefix, err)
		}
	}
	return nil
}

func (d *Storage) Exists(ctx context.Context, fileName string) (bool, error) {
	_, err := os.Stat(path.Join(d.cwd, fileName))
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (d *Storage) SubStorage(dp string, relative bool) storages.Storager {
	dirPath := dp
	if relative {
		dirPath = path.Join(d.cwd, dp)
	}
	return &Storage{
		cwd:      dirPath,
		dirMode:  d.dirMode,
		fileMode: d.fileMode,
	}
}
