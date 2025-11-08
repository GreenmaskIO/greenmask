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

package dump

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/v1/internal/common/dump/processor"
	"github.com/greenmaskio/greenmask/v1/internal/common/heartbeat"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dump/introspect"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dump/schema"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dump/taskproducer"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/metadata"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	engineName = "mysql"
)

// Dump it's responsible for initialization and perform the whole
// dump procedure of mysql instance.
type Dump struct {
	dumpID     commonmodels.DumpID
	st         storages.Storager
	vc         *validationcollector.Collector
	cfg        *config.Config
	connConfig *mysqlmodels.ConnConfig
	registry   *registry.TransformerRegistry
}

func NewDump(
	cfg *config.Config,
	registry *registry.TransformerRegistry,
	st storages.Storager,
) (*Dump, error) {
	dumpID := commonmodels.NewDumpID()
	st = storages.SubStorageWithDumpID(st, dumpID)
	vc := validationcollector.NewCollectorWithMeta(
		commonmodels.MetaKeyDumpID, dumpID,
		commonmodels.MetaKeyEngine, "mysql",
	)
	return &Dump{
		cfg:      cfg,
		st:       st,
		dumpID:   dumpID,
		vc:       vc,
		registry: registry,
	}, nil
}

func (d *Dump) connect() (*sql.DB, error) {
	connConfig, err := d.cfg.Dump.MysqlConfig.Options.ConnectionConfig()
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

func (d *Dump) Run(ctx context.Context) (err error) {
	startedAt := time.Now()
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

	i := introspect.NewIntrospector(&d.cfg.Dump.MysqlConfig.Options)
	if err := i.Introspect(ctx, tx); err != nil {
		return fmt.Errorf("introspect mysql server: %w", err)
	}

	tp, err := taskproducer.New(
		i,
		d.cfg.Dump.Transformation.ToTransformationConfig(),
		d.registry,
		*d.connConfig,
		d.st,
	)
	if err != nil {
		return fmt.Errorf("create task producer: %w", err)
	}

	hbw := heartbeat.NewWorker(heartbeat.NewWriter(d.st))

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(hbw.Run(ctx))
	defer func() {
		// Send termination signal to heartbeat worker.
		// If there is no error, then we mark it as done.
		status := heartbeat.StatusDone
		if err != nil {
			// if there is an error, we mark it as failed.
			status = heartbeat.StatusFailed
		}
		hbw.Terminate(status)
		// Wait for heartbeat worker to finish.
		if err := eg.Wait(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("failed to wait for heartbeat worker")
		}
	}()

	sd := schema.New(d.st, &d.cfg.Dump.MysqlConfig.Options)
	dumper := processor.NewDefaultDumpProcessor(tp, sd).
		SetJobs(1)
	defer func() {
		_ = utils.PrintValidationWarnings(ctx, d.vc, nil, false)
	}()
	dumpStats, err := dumper.Run(ctx, d.vc)
	if err != nil {
		return fmt.Errorf("run dumper: %w", err)
	}
	completedAt := time.Now()
	if err = metadata.WriteMetadata(
		ctx, d.st, engineName, d.cfg.Dump.Transformation.ToTransformationConfig(),
		startedAt, completedAt, dumpStats, i.GetCommonTables(),
	); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	return nil
}
