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

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/dump/processor"
	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/dump/introspect"
	schemadump "github.com/greenmaskio/greenmask/pkg/mysql/dump/schema"
	"github.com/greenmaskio/greenmask/pkg/mysql/dump/taskproducer"
	"github.com/greenmaskio/greenmask/pkg/mysql/metadata"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
	"github.com/greenmaskio/greenmask/pkg/storages"
)

type Option func(dump *Dump) error

func WithFilter(
	filter models.TaskProducerFilter,
) Option {
	return func(dump *Dump) error {
		dump.filter = &filter
		return nil
	}
}

func WithSaveOriginal(
	saveOriginal bool,
) Option {
	return func(dump *Dump) error {
		dump.saveOriginal = saveOriginal
		return nil
	}
}

func WithRowsLimit(
	limit int64,
) Option {
	return func(dump *Dump) error {
		dump.rowsLimit = limit
		return nil
	}
}

func WithDataOnly() Option {
	return func(dump *Dump) error {
		dump.dataOnly = true
		return nil
	}
}

func WithSchemaOnly() Option {
	return func(dump *Dump) error {
		dump.schemaOnly = true
		return nil
	}
}

func WithCompression(
	enabled bool,
	pgzip bool,
) Option {
	return func(dump *Dump) error {
		dump.compressionEnabled = enabled
		dump.compressionPgzip = pgzip
		return nil
	}
}

func WithDumpID(
	dumpID models.DumpID,
) Option {
	return func(dump *Dump) error {
		dump.dumpID = dumpID
		return nil
	}
}

func WithSynchronizeTx(
	lockTimeout time.Duration,
) Option {
	return func(dump *Dump) error {
		dump.synchronizeTx = true
		dump.lockTimeout = lockTimeout
		return nil
	}
}

func WithTransformedTablesOnly() Option {
	return func(dump *Dump) error {
		dump.transformedTablesOnly = true
		return nil
	}
}

func WithFormat(format models.DumpFormat) Option {
	return func(dump *Dump) error {
		dump.format = format
		return nil
	}
}

func getMySQLDumpFilter(cfg config.Validate) (Option, error) {
	filters := make([]models.TableFilter, 0, len(cfg.Tables))
	for i := range cfg.Tables {
		tf, err := models.NewTableFilterItemFromString(cfg.Tables[i])
		if err != nil {
			return nil, fmt.Errorf("create table filter from string %q: %w", cfg.Tables[i], err)
		}
		filters = append(filters, tf)
	}
	return WithFilter(models.TaskProducerFilter{Tables: filters}), nil
}

func GetMySQLDumpOpts(cfg *config.Config) []Option {
	var opts []Option
	if cfg.Dump.Options.DataOnly {
		opts = append(opts, WithDataOnly())
	}
	if cfg.Dump.Options.SchemaOnly {
		opts = append(opts, WithSchemaOnly())
	}

	filter := models.TaskProducerFilter{}
	for _, t := range cfg.Dump.Options.ExcludeTableData {
		tf, err := models.NewTableFilterItemFromString(t)
		if err != nil {
			log.Warn().Err(err).Str("table", t).Msg("invalid table name in exclude-table-data")
			continue
		}
		filter.ExcludedTableData = append(filter.ExcludedTableData, tf)
	}

	if len(filter.ExcludedTableData) > 0 {
		opts = append(opts, WithFilter(filter))
	}

	format := cfg.Dump.MysqlConfig.DumpFormat
	if format == "" {
		format = models.DumpFormatInsert
	}
	opts = append(opts, WithFormat(format))
	opts = append(opts, WithCompression(cfg.Dump.Options.Compress, cfg.Dump.Options.Pgzip))
	return opts
}

func GetMySQLDumpOptsWithValidate(cfg *config.Config) ([]Option, error) {
	var opts []Option
	if cfg.Validate.Diff {
		opts = append(opts, WithSaveOriginal(true))
	}
	if cfg.Validate.RowsLimit > 0 {
		opts = append(opts, WithRowsLimit(int64(cfg.Validate.RowsLimit)))
	}
	if cfg.Validate.OnlyTransformed {
		opts = append(opts, WithTransformedTablesOnly())
	}
	if len(cfg.Validate.Tables) > 0 {
		filterOpt, err := getMySQLDumpFilter(cfg.Validate)
		if err != nil {
			return nil, fmt.Errorf("get mysql dump filter: %w", err)
		}
		opts = append(opts, filterOpt)
	}
	opts = append(opts, WithCompression(cfg.Dump.Options.Compress, cfg.Dump.Options.Pgzip))
	opts = append(opts, GetMySQLDumpOpts(cfg)...)
	return opts, nil
}

// Dump it's responsible for initialization and perform the whole
// dump procedure of mysql instance.
type Dump struct {
	dumpID             models.DumpID
	introsp            interfaces.Introspector
	st                 interfaces.Storager
	cfg                *config.Config
	connConfig         *mysqlmodels.ConnConfig
	registry           *registry.TransformerRegistry
	filter             *models.TaskProducerFilter
	saveOriginal       bool
	rowsLimit          int64
	dataOnly           bool
	schemaOnly         bool
	compressionEnabled bool
	compressionPgzip   bool
	// transformedTablesOnly - dump only transformed tables. This is used in validate command.
	transformedTablesOnly bool
	synchronizeTx         bool
	lockTimeout           time.Duration
	startedAt             time.Time
	dumpStats             models.DumpStat
	hbw                   *heartbeat.Worker
	hbwEg                 *errgroup.Group
	txPool                *pool.ConsistentTxPool
	format                models.DumpFormat
	cmd                   utils.CmdProducer
	dumpedDatabaseSchema  []models.DumpedDatabaseSchemaStat
}

func NewDump(
	cfg *config.Config,
	registry *registry.TransformerRegistry,
	st interfaces.Storager,
	cmd utils.CmdProducer,
	opts ...Option,
) (*Dump, error) {
	dumpID := models.NewDumpID()
	st = storages.SubStorageWithDumpID(st, dumpID)
	res := &Dump{
		cfg:      cfg,
		st:       st,
		dumpID:   dumpID,
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

func (d *Dump) startPool(ctx context.Context) error {
	jobs := 1
	if d.cfg.Dump.Options.Jobs > 0 {
		jobs = d.cfg.Dump.Options.Jobs
	}

	var poolOpts []pool.Option
	if d.cfg.Dump.MysqlConfig.PoolHeartbeatInterval > 0 {
		poolOpts = append(poolOpts, pool.WithHeartbeat(d.cfg.Dump.MysqlConfig.PoolHeartbeatInterval))
		poolOpts = append(poolOpts, pool.WithHeartbeatTimeout(d.cfg.Dump.MysqlConfig.PoolHeartbeatInterval))
	}
	if d.synchronizeTx {
		// TODO: Implement synchronization if needed, currently it is always performed in p.Init()
	}
	connCfg, err := d.cfg.Dump.MysqlConfig.ConnectionConfig()
	if err != nil {
		return fmt.Errorf("get connection config: %w", err)
	}
	var ok bool
	d.connConfig, ok = connCfg.(*mysqlmodels.ConnConfig)
	if !ok {
		return fmt.Errorf("invalid connection config type")
	}
	d.txPool = pool.NewConsistentTxPool(connCfg, jobs, poolOpts...)
	if err := d.txPool.Init(ctx); err != nil {
		return fmt.Errorf("start transaction pool: %w", err)
	}
	return nil
}

func (d *Dump) Init(ctx context.Context) error {
	if err := d.startPool(ctx); err != nil {
		return fmt.Errorf("start transaction pool: %w", err)
	}
	return nil
}

func (d *Dump) Done(ctx context.Context) error {
	if d.txPool != nil {
		if err := d.txPool.Close(ctx); err != nil {
			return fmt.Errorf("close transaction pool: %w", err)
		}
	}
	return nil
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

func (d *Dump) Introspect(ctx context.Context) (err error) {
	ctx = validationcollector.WithMeta(ctx,
		models.MetaKeyDumpID, d.dumpID,
		models.MetaKeyEngine, models.DBMSEngineMySQL,
	)

	d.introsp, err = introspect.NewIntrospector(&d.cfg.Dump.Options)
	if err != nil {
		return fmt.Errorf("new introspector: %w", err)
	}
	if err := d.introsp.Introspect(ctx, d.txPool.GetMetaTx()); err != nil {
		return fmt.Errorf("introspect mysql server: %w", err)
	}

	return nil
}

func (d *Dump) IntrospectAndGetTables(ctx context.Context) ([]models.Table, error) {
	if err := d.Introspect(ctx); err != nil {
		return nil, fmt.Errorf("introspect mysql server: %w", err)
	}
	return d.introsp.GetCommonTables(), nil
}

func (d *Dump) SchemaDump(ctx context.Context) ([]models.DumpedDatabaseSchemaStat, error) {
	settings := d.introsp.GetSchemaRelatedSettings()
	envs, err := d.cfg.Dump.MysqlConfig.Env()
	if err != nil {
		return nil, fmt.Errorf("get environment variables: %w", err)
	}
	options := d.cfg.Dump.MysqlConfig.Params()
	options = append(options, d.cfg.Dump.MysqlConfig.VendorOptions...)
	sd := schemadump.New(d.cmd, d.st, envs, options, settings, d.compressionEnabled, d.compressionPgzip)
	res, err := sd.DumpSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("dump schema: %w", err)
	}
	return res, nil
}

func (d *Dump) DataDump(ctx context.Context) (err error) {
	if d.introsp == nil {
		return fmt.Errorf("introspector is not initialized")
	}

	var taskProducerOpts []taskproducer.Option
	if d.filter != nil {
		taskProducerOpts = append(taskProducerOpts, taskproducer.WithFilter(*d.filter))
	}
	if d.saveOriginal {
		taskProducerOpts = append(taskProducerOpts, taskproducer.WithSaveOriginalData())
	}
	if d.rowsLimit > 0 {
		taskProducerOpts = append(taskProducerOpts, taskproducer.WithRowLimit(d.rowsLimit))
	}
	if d.compressionEnabled {
		taskProducerOpts = append(taskProducerOpts, taskproducer.WithCompressionEnabled())
		if d.compressionPgzip {
			taskProducerOpts = append(taskProducerOpts, taskproducer.WithCompressionPgzip())
		}
	}
	if d.transformedTablesOnly {
		taskProducerOpts = append(taskProducerOpts, taskproducer.WithTransformedTablesOnly())
	}
	taskProducerOpts = append(taskProducerOpts, taskproducer.WithDumpFormat(d.format))
	taskProducerOpts = append(taskProducerOpts,
		taskproducer.WithMaxInsertStatementSize(d.cfg.Dump.MysqlConfig.MaxInsertStatementSize))

	tp, err := taskproducer.New(
		d.introsp,
		d.cfg.Dump.Transformation.ToTransformationConfig(),
		d.registry,
		*d.connConfig,
		d.st,
		d.txPool,
		taskProducerOpts...,
	)
	if err != nil {
		return fmt.Errorf("create task producer: %w", err)
	}

	jobs := 1
	if d.cfg.Dump.Options.Jobs > 0 {
		jobs = d.cfg.Dump.Options.Jobs
	}

	dumper, err := processor.NewDefaultDataDumpProcessor(tp, processor.WithJobs(jobs))
	if err != nil {
		return fmt.Errorf("create dump processor: %w", err)
	}
	d.dumpStats, err = dumper.Run(ctx)
	if err != nil {
		return fmt.Errorf("run dumper: %w", err)
	}

	return nil
}

func (d *Dump) GetDumpMetadata(completedAt time.Time) (models.Metadata, error) {
	dataDump := models.NewDataDumpMetadata(
		d.cfg.Dump.Transformation.ToTransformationConfig(),
		d.getKindsTopologicalOrder(),
		d.dumpStats,
	)
	schemaDump := models.NewSchemaDumpMetadata(
		d.dumpedDatabaseSchema,
	)

	meta := models.NewMetadata(
		models.DBMSEngineMySQL,
		d.startedAt,
		completedAt,
		d.cfg.Dump.Description,
		d.cfg.Dump.Tag,
		d.introsp.GetCommonTables(),
		dataDump,
		schemaDump,
		d.introsp.GetMatchedDatabases(),
	)
	return meta, nil
}

func (d *Dump) WriteMetadata(ctx context.Context) (err error) {
	meta, err := d.GetDumpMetadata(time.Now())
	if err != nil {
		return fmt.Errorf("get dump metadata: %w", err)
	}
	if err = metadata.WriteMetadata(ctx, d.st, meta); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}
	return nil
}

func (d *Dump) Run(ctx context.Context) (err error) {
	d.startedAt = time.Now()
	ctx = validationcollector.WithMeta(ctx,
		models.MetaKeyDumpID, d.dumpID,
		models.MetaKeyEngine, models.DBMSEngineMySQL,
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

	log.Ctx(ctx).Debug().
		Str("format", string(d.format)).
		Msg("using dump format")

	d.StartHBWorker(ctx)
	defer func() {
		if stopHbErr := d.StopHBWorker(ctx, err); stopHbErr != nil {
			log.Ctx(ctx).Warn().Err(stopHbErr).Msg("failed to stop heartbeat worker")
		}
	}()

	if !d.dataOnly {
		// TODO: You need to implement a wrapper that does not release a lock until
		//       schema dump is not finished. This requires to achieve consistent
		//       snapshot for data and schema dump.
		log.Ctx(ctx).Debug().
			Bool("data_only", d.dataOnly).
			Bool("schema_only", d.schemaOnly).
			Msg("dumping schema")
		var schemaStats []models.DumpedDatabaseSchemaStat
		if schemaStats, err = d.SchemaDump(ctx); err != nil {
			return fmt.Errorf("dump schema: %w", err)
		}
		d.dumpedDatabaseSchema = schemaStats
	}

	if !d.schemaOnly {
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

func (d *Dump) getKindsTopologicalOrder() map[models.ObjectKind][]models.TaskID {
	res := make(map[models.ObjectKind][]models.TaskID)
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

func (d *Dump) GetDumpID() models.DumpID {
	return d.dumpID
}
