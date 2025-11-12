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
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	mysqldump "github.com/greenmaskio/greenmask/v1/internal/mysql/cmdrun/dump"
)

const (
	engineNameMySQL    = "mysql"
	engineNamePostgres = "postgresql"
)

var (
	errUnsupportedEngine  = errors.New("unsupported DBMS engine")
	errEngineNotSpecified = errors.New("dbms engine is not specified")
)

func getOptions(_ *config.Config) []mysqldump.Option {
	return []mysqldump.Option{
		mysqldump.WithCompression(true, true),
	}
}

func setupContext(ctx context.Context, cfg *config.Config) context.Context {
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, cfg.Engine).Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(commonmodels.MetaKeyEngine, cfg.Engine)
	ctx = validationcollector.WithCollector(ctx, vc)
	return ctx
}

func setupInfrastructure(cfg *config.Config) error {
	if err := commonutils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	if cfg.Engine == "" {
		return fmt.Errorf("specify dbms engine in \"engine\" key in the config: %w", errEngineNotSpecified)
	}
	return nil
}

// RunDump - runs dump for the specified DBMS engine.
func RunDump(cfg *config.Config) error {
	ctx := context.Background()
	ctx = setupContext(ctx, cfg)
	if err := setupInfrastructure(cfg); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	st, err := commonutils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	switch cfg.Engine {
	case engineNameMySQL:
		opts := getOptions(cfg)
		if err := mysqldump.RunDump(ctx, cfg, st, opts...); err != nil {
			return fmt.Errorf("mysql engine dump: %w", err)
		}
	case engineNamePostgres:
		panic("not implemented yet")
	default:
		return fmt.Errorf("engine \"%s\" is not supported: %w", cfg.Engine, errUnsupportedEngine)
	}
	return nil
}
