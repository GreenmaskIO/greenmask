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

package utils

import (
	"context"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

func GetStorage(ctx context.Context, cfg *config.Config) (interfaces.Storager, error) {
	st, err := storages.Get(
		ctx,
		cfg.Storage.Type,
		cfg.Storage.S3.ToS3Config(),
		cfg.Storage.Directory.ToDirectoryConfig(),
		cfg.Log.Level,
	)
	if err != nil {
		return nil, err
	}
	return st, nil
}
