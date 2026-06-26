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

package builder

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/azure"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/storages/s3"
	sshstorage "github.com/greenmaskio/greenmask/internal/storages/ssh"
)

const (
	DirectoryStorageType = "directory"
	S3StorageType        = "s3"
	AzureStorageType     = "azure"
	SSHStorageType       = "ssh"
)

func GetStorage(ctx context.Context, stCfg *domains.StorageConfig, logCgf *domains.LogConfig) (
	storages.Storager, error,
) {
	log.Ctx(ctx).Debug().Str("type", stCfg.Type).Msg("creating storage")

	switch stCfg.Type {
	case DirectoryStorageType:
		if err := stCfg.Directory.Validate(); err != nil {
			return nil, fmt.Errorf("directory storage config validation failed: %w", err)
		}
		return directory.NewStorage(stCfg.Directory)
	case S3StorageType:
		return s3.NewStorage(ctx, stCfg.S3, logCgf.Level)
	case AzureStorageType:
		if err := stCfg.Azure.Validate(); err != nil {
			return nil, fmt.Errorf("azure storage config validation failed: %w", err)
		}
		return azure.NewStorage(ctx, stCfg.Azure, logCgf.Level)
	case SSHStorageType:
		if err := stCfg.SSH.Validate(); err != nil {
			return nil, fmt.Errorf("ssh storage config validation failed: %w", err)
		}
		return sshstorage.NewStorage(stCfg.SSH)
	}
	return nil, fmt.Errorf("unknown storage type: '%s'", stCfg.Type)
}
