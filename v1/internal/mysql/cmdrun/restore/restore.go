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
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/restore/processor"
	"github.com/greenmaskio/greenmask/v1/internal/common/restore/taskmapper"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/metadata"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/restore/schema"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/restore/taskproducer"
)

// Restore it's responsible for initialization and perform the whole
// dump procedure of mysql instance.
type Restore struct {
	dumpID     commonmodels.DumpID
	st         commonininterfaces.Storager
	vc         *validationcollector.Collector
	cfg        *config.Config
	connConfig *mysqlmodels.ConnConfig
}

func NewRestore(
	cfg *config.Config,
	st commonininterfaces.Storager,
	dumpID commonmodels.DumpID,
) *Restore {
	vc := validationcollector.NewCollectorWithMeta(
		commonmodels.MetaKeyDumpID, dumpID,
		commonmodels.MetaKeyEngine, "mysql",
	)
	return &Restore{
		cfg:    cfg,
		st:     st,
		dumpID: dumpID,
		vc:     vc,
	}
}

func (d *Restore) connect() (*sql.DB, error) {
	connConfig, err := d.cfg.Restore.MysqlConfig.Options.ConnectionConfig()
	if err != nil {
		return nil, fmt.Errorf("get connection config: %w", err)
	}
	var ok bool
	d.connConfig, ok = connConfig.(*mysqlmodels.ConnConfig)
	if !ok {
		panic("invalid connection config type")
	}
	connStr, err := connConfig.URI()
	if err != nil {
		return nil, fmt.Errorf("get connection URI: %w", err)
	}
	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	return conn, nil
}

func (d *Restore) Run(ctx context.Context) error {
	conn, err := d.connect()
	if err != nil {
		return fmt.Errorf("connect to mysql: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("failed to close mysql connection")
		}
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("failed to rollback transaction")
		}
	}()

	meta, err := metadata.ReadMetadata(ctx, d.st)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	taskResolver := taskmapper.NewTaskResolver()

	var tp commonininterfaces.RestoreTaskProducer
	if d.cfg.Restore.Options.RestoreInOrder {
		log.Ctx(ctx).Info().Msg("restoring tables in topological")
		tp = taskproducer.NewWithOrder(meta, d.st, d.cfg.Restore.MysqlConfig.Options.ConnectionOpts, taskResolver)
	} else {
		tp = taskproducer.New(meta, d.st, d.cfg.Restore.MysqlConfig.Options.ConnectionOpts)
	}

	sr := schema.NewRestorer(d.st, &d.cfg.Restore.MysqlConfig.Options)

	if err := processor.NewDefaultRestoreProcessor(ctx, tp, sr, processor.Config{
		Jobs:           d.cfg.Restore.Options.Jobs,
		RestoreInOrder: d.cfg.Restore.Options.RestoreInOrder,
	}).Run(ctx); err != nil {
		return fmt.Errorf("run restore processor: %w", err)
	}
	return nil
}
