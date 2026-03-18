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

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/restore/processor"
	"github.com/greenmaskio/greenmask/pkg/common/restore/taskmapper"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/metadata"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/schema"
	taskproducer2 "github.com/greenmaskio/greenmask/pkg/mysql/restore/taskproducer"
)

// Restore it's responsible for initialization and perform the whole
// dump procedure of mysql instance.
type Restore struct {
	dumpID     models.DumpID
	st         interfaces.Storager
	vc         *validationcollector.Collector
	cfg        *config.Config
	connConfig *mysqlmodels.ConnConfig
	cmd        utils.CmdProducer
}

func NewRestore(
	cfg *config.Config,
	st interfaces.Storager,
	dumpID models.DumpID,
	cmd utils.CmdProducer,
) *Restore {
	vc := validationcollector.NewCollectorWithMeta(
		models.MetaKeyDumpID, dumpID,
		models.MetaKeyEngine, "mysql",
	)
	return &Restore{
		cfg:    cfg,
		st:     st,
		dumpID: dumpID,
		vc:     vc,
		cmd:    cmd,
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
	if err := d.cfg.Restore.MysqlConfig.Options.Validate(); err != nil {
		return fmt.Errorf("validate mysql options: %w", err)
	}
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

	var tp interfaces.RestoreTaskProducer
	if d.cfg.Restore.Options.RestoreInOrder {
		log.Ctx(ctx).Info().Msg("restoring tables in topological")
		tp = taskproducer2.NewWithOrder(meta, d.st, d.cfg.Restore.MysqlConfig.Options, taskResolver)
	} else {
		tp = taskproducer2.New(meta, d.st, d.cfg.Restore.MysqlConfig.Options)
	}

	sr := schema.NewRestorer(d.st, &d.cfg.Restore.MysqlConfig.Options, d.cmd)

	if err := processor.NewDefaultRestoreProcessor(ctx, tp, sr, processor.Config{
		Jobs:           d.cfg.Restore.Options.Jobs,
		RestoreInOrder: d.cfg.Restore.Options.RestoreInOrder,
		DataOnly:       d.cfg.Restore.Options.DataOnly,
		SchemaOnly:     d.cfg.Restore.Options.SchemaOnly,
	}).Run(ctx); err != nil {
		return fmt.Errorf("run restore processor: %w", err)
	}
	return nil
}
