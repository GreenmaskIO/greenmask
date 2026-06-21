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

package restore

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	restorestorage "github.com/greenmaskio/greenmask/pkg/common/restore/storage"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
)

const (
	DumpIDLatest         = "latest"
	MetadataJsonFileName = "metadata.json"
)

func RunRestore(
	ctx context.Context, cfg *config.Config, st core.Storager, dumpIDArg string,
) error {
	dumpID := core.DumpID(dumpIDArg)
	if err := dumpID.Validate(); err != nil {
		return fmt.Errorf("validate dumpID: %w", err)
	}
	st, err := restorestorage.GetStorageByDumpID(ctx, st, dumpID, cfg.Common.HeartbeatInterval)
	if err != nil {
		return fmt.Errorf("get storage by dumpID: %w", err)
	}
	if err := NewRestore(cfg, st, dumpID, utils.NewDefaultCmdProducer()).Run(ctx); err != nil {
		return fmt.Errorf("run restore process: %w", err)
	}
	return nil
}
