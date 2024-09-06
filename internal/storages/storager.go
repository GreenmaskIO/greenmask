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
	"io"

	"github.com/greenmaskio/greenmask/internal/storages/domains"
)

type Storager interface {
	// GetCwd - get current working directory (CWD) path
	GetCwd() string
	// Dirname - returns current dirname without prefix
	Dirname() string
	// ListDir - walking through storage and returns directories and files in the cwd
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	// GetObject - returns ReadCloser by the provided path
	GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error)
	// PutObject - puts data to the provided file path
	PutObject(ctx context.Context, filePath string, body io.Reader) error
	// Delete - delete list of objects by the provided paths
	Delete(ctx context.Context, filePaths ...string) error
	// DeleteAll - delete all objects by the provided path prefix
	DeleteAll(ctx context.Context, pathPrefix string) error
	// Exists - check object existence
	Exists(ctx context.Context, fileName string) (bool, error)
	// SubStorage - get new Storage instance with the samo config but change current cwd via subPath
	// If relative == true then path is sub folder in cwd
	SubStorage(subPath string, relative bool) Storager
	// Stat - get the metadata info about object from the storage
	Stat(fileName string) (*domains.ObjectStat, error)
}
