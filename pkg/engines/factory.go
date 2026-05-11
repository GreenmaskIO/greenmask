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

package engines

import (
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
	mysqldump "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	mysqlrestore "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/restore"
	pgdump "github.com/greenmaskio/greenmask/pkg/postgresql/cmdrun/dump"
	pgrestore "github.com/greenmaskio/greenmask/pkg/postgresql/cmdrun/restore"
)

var errUnsupportedEngine = errors.New("unsupported DBMS engine")

// NewDumper returns the engine-specific dump orchestrator for cfg.Engine.
func NewDumper(cfg *config.Config, st interfaces.Storager) (Dumper, error) {
	switch cfg.Engine {
	case models.DBMSEngineMySQL:
		return mysqldump.NewDump(
			cfg,
			registry.DefaultTransformerRegistry,
			st,
			utils.NewDefaultCmdProducer(),
			mysqldump.GetMySQLDumpOpts(cfg)...,
		)
	case models.DBMSEnginePostgreSQL:
		return pgdump.New(cfg, st)
	default:
		return nil, fmt.Errorf("engine %q: %w", cfg.Engine, errUnsupportedEngine)
	}
}

// NewRestorer returns the engine-specific restore orchestrator for cfg.Engine.
// st is the root storager; dumpID resolution ("latest" → concrete ID) happens inside Run.
func NewRestorer(cfg *config.Config, st interfaces.Storager, dumpID models.DumpID) (Restorer, error) {
	switch cfg.Engine {
	case models.DBMSEngineMySQL:
		return mysqlrestore.NewRestore(cfg, st, dumpID, utils.NewDefaultCmdProducer()), nil
	case models.DBMSEnginePostgreSQL:
		return pgrestore.New(cfg, st, dumpID)
	default:
		return nil, fmt.Errorf("engine %q: %w", cfg.Engine, errUnsupportedEngine)
	}
}

// NewValidator returns the engine-specific validate orchestrator for cfg.Engine.
// st is expected to be a validate.Storage (no-write). The validate result is
// collected via validationcollector in the context after Run returns.
func NewValidator(cfg *config.Config, st interfaces.Storager) (Validator, error) {
	switch cfg.Engine {
	case models.DBMSEngineMySQL:
		opts, err := mysqldump.GetMySQLDumpOptsWithValidate(cfg)
		if err != nil {
			return nil, fmt.Errorf("get mysql validate opts: %w", err)
		}
		return mysqldump.NewDump(
			cfg,
			registry.DefaultTransformerRegistry,
			st,
			utils.NewDefaultCmdProducer(),
			opts...,
		)
	case models.DBMSEnginePostgreSQL:
		return pgdump.NewValidator(cfg, st)
	default:
		return nil, fmt.Errorf("engine %q: %w", cfg.Engine, errUnsupportedEngine)
	}
}
