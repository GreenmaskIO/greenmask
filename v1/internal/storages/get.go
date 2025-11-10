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

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
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

// Get returns a storage based on the configuration.
func Get(
	ctx context.Context,
	storageType string,
	s3Cfg s3.S3Config,
	directoryCfg directory.DirectoryConfig,
	logLevel string,
) (interfaces.Storager, error) {
	switch storageType {
	case directoryStorageType:
		return directory.New(directoryCfg)
	case s3StorageType:
		return s3.New(ctx, s3Cfg, logLevel)
	}
	return nil, fmt.Errorf("storage type %s: %w", storageType, errUnknownStorageType)
}

func SubStorageWithDumpID(st interfaces.Storager, dumpID commonmodels.DumpID) interfaces.Storager {
	return st.SubStorage(string(dumpID), true)
}
