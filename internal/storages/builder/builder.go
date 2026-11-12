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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/azure"
	"github.com/greenmaskio/storages/directory"
	"github.com/greenmaskio/storages/s3"
	"github.com/greenmaskio/storages/ssh"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
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
	slogger := logger.NewSlog("storages")

	switch stCfg.Type {
	case DirectoryStorageType:
		if err := stCfg.Directory.Validate(); err != nil {
			return nil, fmt.Errorf("directory storage config validation failed: %w", err)
		}
		return directory.NewStorage(toDirectory(stCfg.Directory), directory.WithLogger(slogger))
	case S3StorageType:
		if err := stCfg.S3.Validate(); err != nil {
			return nil, fmt.Errorf("s3 storage config validation failed: %w", err)
		}
		opts := []s3.Option{s3.WithLogger(slogger)}
		if logCgf.Level == zerolog.LevelDebugValue {
			opts = append(opts, s3.WithAWSLogLevel(aws.LogRequest|aws.LogRetries))
		}
		if stCfg.S3.RoleArn != "" {
			// The library assumes the role lazily, so this cannot report success.
			log.Ctx(ctx).Info().Str("RoleArn", stCfg.S3.RoleArn).Msg("s3 storage will assume role on first request")
		}
		return s3.New(ctx, toS3(stCfg.S3), opts...)
	case AzureStorageType:
		if err := stCfg.Azure.Validate(); err != nil {
			return nil, fmt.Errorf("azure storage config validation failed: %w", err)
		}
		return azure.New(ctx, toAzure(stCfg.Azure), azure.WithLogger(slogger))
	case SSHStorageType:
		if err := stCfg.SSH.Validate(); err != nil {
			return nil, fmt.Errorf("ssh storage config validation failed: %w", err)
		}
		return ssh.New(toSSH(stCfg.SSH), ssh.WithLogger(slogger))
	}
	return nil, fmt.Errorf("unknown storage type: '%s'", stCfg.Type)
}
