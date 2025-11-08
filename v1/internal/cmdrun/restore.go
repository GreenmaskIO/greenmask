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

package cmdrun

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	mysqlrestore "github.com/greenmaskio/greenmask/v1/internal/mysql/cmdrun/restore"
)

// RunRestore - runs restore for the specified DBMS engine and dump ID.
func RunRestore(cfg *config.Config, dumpID string) error {
	ctx := context.Background()
	st, err := commonutils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	if err := commonutils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	if cfg.Engine == "" {
		return fmt.Errorf("specify dbms engine in \"engine\" key in the config: %w", errEngineNotSpecified)
	}
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, cfg.Engine).Logger().WithContext(ctx)
	switch cfg.Engine {
	case engineNameMySQL:
		if err := mysqlrestore.RunRestore(ctx, cfg, st, dumpID); err != nil {
			return fmt.Errorf("mysql engine restore: %w", err)
		}
	case engineNamePostgres:
		panic("not implemented yet")
	default:
		return fmt.Errorf("engine \"%s\" is not supported: %w", cfg.Engine, errUnsupportedEngine)
	}
	return nil
}
