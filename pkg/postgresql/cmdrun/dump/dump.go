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
	"fmt"
	"time"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/storages"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type Option func(dump *Dump) error

func WithSmth(
	smth any,
) Option {
	return func(dump *Dump) error {
		return nil
	}
}

// Dump is the PostgreSQL dump orchestrator placeholder.
// All methods return "not implemented yet" until the PostgreSQL port is complete.
type Dump struct {
	dumpID               core.DumpID
	cfg                  *config.Config
	st                   core.Storager
	registry             *registry.TransformerRegistry
	cmd                  utils.CmdProducer
	hbw                  *heartbeat.Worker
	hbwEg                *errgroup.Group
	startedAt            time.Time
	dumpStats            core.DataDumpStat
	dumpedDatabaseSchema []core.SchemaDumpStat
	dataOnly             bool
	schemaOnly           bool
	currentSnapshot      string
}

// New returns a Dump for use as an engines.Dumper.
func New(
	cfg *config.Config,
	registry *registry.TransformerRegistry,
	st core.Storager,
	cmd utils.CmdProducer,
	opts ...Option,
) (*Dump, error) {
	dumpID := core.NewDumpID()
	st = storages.SubStorageWithDumpID(st, dumpID)
	res := &Dump{
		cfg:      cfg,
		st:       st,
		registry: registry,
		cmd:      cmd,
	}
	for i, opt := range opts {
		if err := opt(res); err != nil {
			return nil, fmt.Errorf("apply dump option %d: %w", i, err)
		}
	}
	return res, nil
}

// NewValidator returns a Dump configured for validation (same placeholder).
func NewValidator(
	cfg *config.Config,
	registry *registry.TransformerRegistry,
	st core.Storager,
	cmd utils.CmdProducer,
) (*Dump, error) {
	return New(cfg, registry, st, cmd)
}

func (d *Dump) connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	pgxdecimal.Register(conn.TypeMap())

	if err := conn.Ping(ctx); err != nil {
		if err := conn.Close(ctx); err != nil {
			log.Debug().Err(err).Msg("unable to close connection")
		}
		return nil, err
	}

	d.dsn = dsn
	return conn, nil
}

func (d *Dump) startMainTx(ctx context.Context, conn *pgx.Conn) (pgx.Tx, error) {
	pgCfg := d.cfg.Dump.PostgresqlConfig
	var isolationLevel = "REPEATABLE READ"
	if pgCfg.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start transaction: %w", err)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel))
	if err != nil {
		if err := tx.Rollback(ctx); err != nil {
			log.Debug().Err(err).Msg("unable to rollback transaction")
		}
		return nil, fmt.Errorf("cannot set transaction isolation level: %w", err)
	}
	rows.Close()

	if pgCfg.Snapshot == "" {
		log.Debug().Msg("performing snapshot export")
		row := tx.QueryRow(ctx, "SELECT pg_export_snapshot()")
		if err := row.Scan(&d.currentSnapshot); err != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Debug().Err(err).Msg("unable to rollback transaction")
			}
			return nil, fmt.Errorf("cannot export snapshot: %w", err)
		}
	} else {
		var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", pgCfg.Snapshot)
		log.Debug().Msgf("performing '%s' snapshot import", pgCfg.Snapshot)
		if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Warn().Err(err).Msg("unable to rollback transaction")
			}
			return nil, fmt.Errorf("cannot import snapshot: %w", err)
		}
	}

	return tx, nil
}

func (d *Dump) Init(ctx context.Context) error {
	connCfg, err := d.cfg.Dump.MysqlConfig.ConnectionConfig(d.cfg.Dump.Options.SSL)
	dsn, err := d.pgDumpOptions.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}
	conn, err := d.connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()
	tx, err := d.startMainTx(ctx, conn)
}

func (d *Dump) Done(_ context.Context) error {
	return errNotImplemented("done")
}

func (d *Dump) StartHBWorker(ctx context.Context) {
	hbInterval := d.cfg.Common.HeartbeatInterval
	if hbInterval <= 0 {
		hbInterval = heartbeat.DefaultWriteInterval
	}
	d.hbw = heartbeat.NewWorker(heartbeat.NewWriter(d.st)).
		SetInterval(hbInterval)
	d.hbwEg, ctx = errgroup.WithContext(ctx)
	d.hbwEg.Go(d.hbw.Run(ctx))
}

func (d *Dump) StopHBWorker(ctx context.Context, err error) error {
	// Send termination signal to heartbeat worker.
	// If there is no error, then we mark it as done.
	status := heartbeat.StatusDone
	if err != nil {
		// if there is an error, we mark it as failed.
		status = heartbeat.StatusFailed
	}
	d.hbw.Terminate(status)
	// Wait for heartbeat worker to finish.
	if err := d.hbwEg.Wait(); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("failed to wait for heartbeat worker")
	}
	return nil
}

func (d *Dump) Introspect(_ context.Context) error {
	return errNotImplemented("introspect")
}

func (d *Dump) IntrospectAndGetTables(_ context.Context) ([]core.Table, error) {
	return nil, errNotImplemented("introspect-tables")
}

func (d *Dump) SchemaDump(_ context.Context) ([]core.SchemaDumpStat, error) {
	return nil, errNotImplemented("schema-dump")
}

func (d *Dump) DataDump(_ context.Context) error {
	return errNotImplemented("data-dump")
}

func (d *Dump) GetDumpMetadata(_ time.Time) (core.Metadata, error) {
	return core.Metadata{}, errNotImplemented("get-metadata")
}

func (d *Dump) WriteMetadata(_ context.Context) error {
	return errNotImplemented("write-metadata")
}

func (d *Dump) sectionEnabled(section core.DumpSection) bool {
	if len(d.sections) == 0 {
		switch section {
		case core.DumpSectionPreData, core.DumpSectionPostData:
			return !d.dataOnly
		case core.DumpSectionData:
			return !d.schemaOnly
		}
		return true
	}
	_, ok := d.sections[section]
	return ok
}

func (d *Dump) Run(ctx context.Context) (err error) {
	d.startedAt = time.Now()
	ctx = validationcollector.WithMeta(ctx,
		core.MetaKeyDumpID, d.dumpID,
		core.MetaKeyEngine, core.DBMSEngineMySQL,
	)

	if err := d.Init(ctx); err != nil {
		return fmt.Errorf("initialize resources: %w", err)
	}
	defer func() {
		if err := d.Done(ctx); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("failed to release resources")
		}
	}()

	if err := d.Introspect(ctx); err != nil {
		return fmt.Errorf("introspect mysql server: %w", err)
	}

	d.StartHBWorker(ctx)
	defer func() {
		if stopHbErr := d.StopHBWorker(ctx, err); stopHbErr != nil {
			log.Ctx(ctx).Warn().Err(stopHbErr).Msg("failed to stop heartbeat worker")
		}
	}()

	if d.sectionEnabled(core.DumpSectionPreData) || d.sectionEnabled(core.DumpSectionPostData) {
		// TODO: You need to implement a wrapper that does not release a lock until
		//       schema dump is not finished. This requires to achieve consistent
		//       snapshot for data and schema dump.
		log.Ctx(ctx).Debug().
			Bool("data_only", d.dataOnly).
			Bool("schema_only", d.schemaOnly).
			Msg("dumping schema")
		var schemaStats []core.SchemaDumpStat
		if schemaStats, err = d.SchemaDump(ctx); err != nil {
			return fmt.Errorf("dump schema: %w", err)
		}
		d.dumpedDatabaseSchema = schemaStats
	}

	if d.sectionEnabled("data") {
		log.Ctx(ctx).Debug().
			Bool("data_only", d.dataOnly).
			Bool("schema_only", d.schemaOnly).
			Msg("dumping data")
		if err := d.DataDump(ctx); err != nil {
			return fmt.Errorf("dump data: %w", err)
		}
	}

	if err = d.WriteMetadata(ctx); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}
	return nil
}

func (d *Dump) getKindsTopologicalOrder() map[core.ObjectKind][]core.TaskID {
	res := make(map[core.ObjectKind][]core.TaskID)
	for _, taskID := range d.dumpStats.RestorationContext.RestorationOrder {
		stat, ok := d.dumpStats.TaskStats[taskID]
		if !ok {
			continue
		}
		kind := stat.ObjectStat.Kind
		res[kind] = append(res[kind], taskID)
	}
	return res
}

func (d *Dump) GetDumpID() core.DumpID {
	return ""
}

func (d *Dump) DumpSample(_ context.Context, _ bool, _ []core.TableFilter) error {
	return errNotImplemented("dump-sample")
}

func (d *Dump) SchemaDiff(_ context.Context) error {
	return errNotImplemented("schema-diff")
}

func (d *Dump) Introspection() []core.Table {
	return nil
}

func (d *Dump) Warnings() []*core.ValidationWarning {
	return nil
}

func errNotImplemented(op string) error {
	return fmt.Errorf("postgresql dump %s: not implemented yet", op)
}
