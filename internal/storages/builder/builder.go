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
	"errors"
	"os"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/storages/s3"
)

func GetStorage(ctx context.Context, stCfg *domains.StorageConfig, logCgf *domains.LogConfig) (
	storages.Storager, error,
) {
	envCfg := os.Getenv("STORAGE_TYPE")
	if stCfg.Directory != nil || envCfg == "directory" {
		return directory.NewStorage(stCfg.Directory)
	} else if stCfg.S3 != nil || envCfg == "s3" {
		return s3.NewStorage(ctx, stCfg.S3, logCgf.Level)
	}
	return nil, errors.New("no one storage was provided")
}
