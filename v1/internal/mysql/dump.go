package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/datadump"
	"github.com/greenmaskio/greenmask/v1/internal/common/heartbeat"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/tabledriver"
	utils2 "github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/introspect"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/schemadumper"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/taskproducer"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	defaultInitTimeout = 30 * time.Second
)

func newMysqlTableDriver(
	vc *validationcollector.Collector,
	table commonmodels.Table,
	columnsTypeOverride map[string]string,
) (commonininterfaces.TableDriver, error) {
	return tabledriver.New(vc, dbmsdriver.New(), &table, columnsTypeOverride)
}

// Dump it's responsible for initialization and perform the whole
// dump procedure of mysql instance.
type Dump struct {
	dumpID     commonmodels.DumpID
	st         storages.Storager
	vc         *validationcollector.Collector
	cfg        *config.Config
	connConfig *mysqlmodels.ConnConfig
	registry   *utils2.TransformerRegistry
}

func NewDump(
	ctx context.Context,
	cfg *config.Config,
	registry *utils2.TransformerRegistry,
	st storages.Storager,
) (*Dump, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultInitTimeout)
	defer cancel()

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

/*
It must:
  - Introspect schema
  - Generate subsets
  - Rewrite a config if some requirements are met (FK inharitance / partitioning)
  - Initialize TableRuntime (transformers, conditions, dump queries)
  - Run heartbeat worker
  - Generate schema dump
  - Generate data dump -> Receives (task producer) -> Produce tasks -> execute on the workerы
  - Generate metadata based on the config, collected tables and some additional data
  - Complete dump

How tasks producer should work?
- Dedicate producer for each type of data object (table, sequences, large objects, etc.)

For tables:
- Produce raw dumper if there is no transformations - need to store data only.
- Produce TransformationPipelineDumper - executes transformer one by one and valudates conditions.
*/
func (d *Dump) Run(ctx context.Context) error {
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

	tp := taskproducer.New(
		i,
		d.cfg.Dump.Transformation.ToTransformationConfig(),
		d.registry,
		*d.connConfig,
		d.st,
	)

	hbw := heartbeat.NewWorker(heartbeat.NewWriter(d.st))
	sd := schemadumper.New(d.st, &d.cfg.Dump.MysqlConfig.Options)

	dumper := datadump.NewDefaultDataDumper(tp, hbw, sd).
		SetJobs(1)

	defer func() {
		_ = utils.PrintValidationWarnings(ctx, d.vc, nil, true)
	}()
	if err := dumper.Run(ctx, d.vc); err != nil {
		return fmt.Errorf("run dumper: %w", err)
	}
	return nil
}
