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
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/taskproducer"
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
	connConfig, err := d.cfg.Restore.MysqlConfig.ConnectionConfig(d.cfg.Restore.Options.SSL)
	if err != nil {
		return nil, fmt.Errorf("get connection config: %w", err)
	}
	d.connConfig = connConfig
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

func (d *Restore) remapDB(meta models.Metadata) (map[string]string, error) {
	remap := d.cfg.Restore.Options.RemapDatabase
	if len(remap) == 0 {
		return nil, nil
	}
	mode := d.cfg.Restore.Options.DatabaseReplaceMode
	if mode == "" {
		mode = models.DatabaseReplaceModeStrict
	}
	switch mode {
	case models.DatabaseReplaceModeStrict:
		for _, db := range meta.Databases {
			if _, ok := remap[db]; !ok {
				return nil, fmt.Errorf("database-replace-mode=strict: database %q has no entry in remap-database", db)
			}
		}
	case models.DatabaseReplaceModeRelaxed:
	default:
		return nil, fmt.Errorf("unknown database-replace-mode %q", mode)
	}
	return remap, nil
}

func (d *Restore) Run(ctx context.Context) error {
	if err := d.cfg.Restore.MysqlConfig.Validate(); err != nil {
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

	remap, err := d.remapDB(meta)
	if err != nil {
		return fmt.Errorf("remapDB: %w", err)
	}

	var tp interfaces.RestoreTaskProducer
	opts := taskproducer.RestoreOptions{
		PrintWarnings:           d.cfg.Restore.MysqlConfig.PrintWarnings,
		MaxFetchWarnings:        d.cfg.Restore.MysqlConfig.MaxFetchWarnings,
		DisableForeignKeyChecks: d.cfg.Restore.MysqlConfig.DisableForeignKeyChecks,
		DisableUniqueChecks:     d.cfg.Restore.MysqlConfig.DisableUniqueChecks,
		InsertIgnore:            d.cfg.Restore.MysqlConfig.InsertIgnore,
		InsertReplace:           d.cfg.Restore.MysqlConfig.InsertReplace,
		MaxInsertStatementSize:  d.cfg.Restore.MysqlConfig.MaxInsertStatementSize,
		DatabaseRemap:           remap,
	}

	if d.cfg.Restore.Options.RestoreInOrder {
		log.Ctx(ctx).Info().Msg("restoring tables in topological")
		tp = taskproducer.NewWithOrder(
			meta, d.st, d.connConfig,
			opts,
			taskResolver,
		)
	} else {
		tp = taskproducer.New(
			meta, d.st, d.connConfig,
			opts,
		)
	}

	schemaOpts := []schema.Option{}
	if d.cfg.Restore.Options.CreateDatabase && len(meta.Databases) > 0 {
		schemaOpts = append(schemaOpts, schema.WithCreateDatabase(conn, meta.Databases))
	}
	if d.cfg.Restore.Options.IfNotExists {
		schemaOpts = append(schemaOpts, schema.WithIfNotExists())
	}
	if len(remap) > 0 {
		schemaOpts = append(schemaOpts, schema.WithDatabaseRemap(remap))
	}
	sr := schema.NewRestorer(d.st, &d.cfg.Restore.MysqlConfig, d.cfg.Restore.Options.SSL, d.cmd, meta.SchemaDump, schemaOpts...)

	if err := processor.NewDefaultRestoreProcessor(ctx, tp, sr, processor.Config{
		Jobs:           d.cfg.Restore.Options.Jobs,
		RestoreInOrder: d.cfg.Restore.Options.RestoreInOrder,
		DataOnly:       d.cfg.Restore.Options.DataOnly,
		SchemaOnly:     d.cfg.Restore.Options.SchemaOnly,
		Section:        d.cfg.Restore.Options.Section,
	}, d.cfg.Restore.Scripts, d.connConfig).Run(ctx); err != nil {
		return fmt.Errorf("run restore processor: %w", err)
	}
	return nil
}
