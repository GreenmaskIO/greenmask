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
	"fmt"

	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/v1/internal/common/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages/directory"
	"github.com/greenmaskio/greenmask/v1/internal/storages/s3"
)

const (
	directoryStorageType = "directory"
	s3StorageType        = "s3"
)

var (
	errUnknownStorageType = fmt.Errorf("unknown storage type")
)

// GetStorage returns a storage based on the configuration.
func GetStorage(
	ctx context.Context,
	stCfg config.Storage,
	logCgf config.Log,
) (storages.Storager, error) {
	switch stCfg.Type {
	case directoryStorageType:
		return directory.NewStorage(directory.NewConfig(*stCfg.Directory))
	case s3StorageType:
		return s3.NewStorage(ctx, s3.NewConfig(*stCfg.S3), logCgf.Level)
	}
	return nil, fmt.Errorf("storage type %s: %w", stCfg.Type, errUnknownStorageType)
}
