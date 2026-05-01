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

package restorers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

const dumperTypeTableData = "table_restorer"

var (
	_ interfaces.Restorer = (*TableDataRestorerCsv)(nil)
)

type TableDataRestorerCsv struct {
	table               *models.Table
	meta                models.RestorationItem
	connConfig          *mysqlmodels.ConnConfig
	st                  interfaces.Storager
	taskResolver        interfaces.TaskMapper
	compress            bool
	pgzip               bool
	printWarnings       bool
	maxFetchWarnings    int
	disableFkChecks     bool
	disableUniqueChecks bool
	insertIgnore        bool
	insertReplace       bool
	db                  *sql.DB
	tx                  *sql.Tx
	execErr             error
}

func (r *TableDataRestorerCsv) Meta() map[string]any {
	return map[string]any{
		models.MetaKeyTableSchema:      r.table.Schema,
		models.MetaKeyTableName:        r.table.Name,
		models.MetaKeyUniqueDumpTaskID: r.DebugInfo(),
	}
}

func (r *TableDataRestorerCsv) DebugInfo() string {
	return commonutils.GetUniqueTaskID(dumperTypeTableData, r.table.Schema, r.table.Name)
}

type TableRestorerConfig struct {
	Compress                bool
	Pgzip                   bool
	PrintWarnings           bool
	MaxFetchWarnings        int
	DisableForeignKeyChecks bool
	DisableUniqueChecks     bool
	InsertIgnore            bool
	InsertReplace           bool
	MaxInsertStatementSize  int
	DatabaseRemap           map[string]string
}

type Option func(v *TableRestorerConfig) error

func WithCompression(
	enabled bool,
	pgzip bool,
) Option {
	return func(v *TableRestorerConfig) error {
		v.Compress = enabled
		v.Pgzip = pgzip
		return nil
	}
}

func WithWarnings(
	printWarnings bool,
	maxFetch int,
) Option {
	return func(v *TableRestorerConfig) error {
		v.PrintWarnings = printWarnings
		v.MaxFetchWarnings = maxFetch
		return nil
	}
}

func WithForeignKeyChecks(enabled bool) Option {
	return func(v *TableRestorerConfig) error {
		v.DisableForeignKeyChecks = enabled
		return nil
	}
}

func WithUniqueChecks(enabled bool) Option {
	return func(v *TableRestorerConfig) error {
		v.DisableUniqueChecks = enabled
		return nil
	}
}

func WithInsertIgnore() Option {
	return func(v *TableRestorerConfig) error {
		if v.InsertReplace {
			return fmt.Errorf("insert-ignore and insert-replace are mutually exclusive")
		}
		v.InsertIgnore = true
		return nil
	}
}

func WithInsertReplace() Option {
	return func(v *TableRestorerConfig) error {
		if v.InsertIgnore {
			return fmt.Errorf("insert-ignore and insert-replace are mutually exclusive")
		}
		v.InsertReplace = true
		return nil
	}
}

func WithMaxInsertStatementSize(size int) Option {
	return func(v *TableRestorerConfig) error {
		v.MaxInsertStatementSize = size
		return nil
	}
}

func WithDatabaseRemap(remap map[string]string) Option {
	return func(v *TableRestorerConfig) error {
		v.DatabaseRemap = remap
		return nil
	}
}

func NewTableDataRestorerCsv(
	meta models.RestorationItem,
	connConfig *mysqlmodels.ConnConfig,
	st interfaces.Storager,
	taskResolver interfaces.TaskMapper,
	opts ...Option,
) (*TableDataRestorerCsv, error) {
	var table models.Table
	if err := json.Unmarshal(meta.ObjectDefinition, &table); err != nil {
		return nil, err
	}
	if err := table.Validate(); err != nil {
		return nil, fmt.Errorf("validate table: %w", err)
	}
	cfg := &TableRestorerConfig{}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, fmt.Errorf("options failed: %w", err)
		}
	}
	if mapped, ok := cfg.DatabaseRemap[table.Schema]; ok {
		table.Schema = mapped
	}

	res := &TableDataRestorerCsv{
		table:               &table,
		meta:                meta,
		connConfig:          connConfig,
		st:                  st,
		taskResolver:        taskResolver,
		compress:            cfg.Compress,
		pgzip:               cfg.Pgzip,
		printWarnings:       cfg.PrintWarnings,
		maxFetchWarnings:    cfg.MaxFetchWarnings,
		disableFkChecks:     cfg.DisableForeignKeyChecks,
		disableUniqueChecks: cfg.DisableUniqueChecks,
		insertIgnore:        cfg.InsertIgnore,
		insertReplace:       cfg.InsertReplace,
	}
	return res, nil
}

func getFileHandlerName(t models.Table) string {
	return fmt.Sprintf("%s__%s", t.Schema, t.Name)
}

func (r *TableDataRestorerCsv) showWarnings(ctx context.Context, db *sql.DB) error {
	return showWarnings(ctx, db, r.printWarnings, r.maxFetchWarnings)
}

func (r *TableDataRestorerCsv) restoreTable(ctx context.Context, tx *sql.Tx) error {
	conflictKeyword := ""
	if r.insertReplace {
		conflictKeyword = "REPLACE "
	} else if r.insertIgnore {
		conflictKeyword = "IGNORE "
	}
	query := fmt.Sprintf(
		`LOAD DATA LOCAL INFILE 'Reader::%s' `+
			"%sINTO TABLE `%s`.`%s` "+
			`FIELDS TERMINATED BY ',' `+
			`ENCLOSED BY '"' `+
			`ESCAPED BY '\\' `+
			`LINES TERMINATED BY '\n'`,
		getFileHandlerName(*r.table),
		conflictKeyword,
		r.table.Schema,
		r.table.Name,
	)

	log.Ctx(ctx).Debug().
		Str(models.MetaKeyQuery, query).
		Msg("restoring table data")

	res, err := tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rowsAffected != r.meta.RecordCount {
		log.Ctx(ctx).Warn().
			Int64("RowsAffected", rowsAffected).
			Int64("ExpectedRecordCount", r.meta.RecordCount).
			Msg("some rows may be skipped: rows affected does not match expected record count")
	}

	return nil
}

func (r *TableDataRestorerCsv) setupTx(ctx context.Context, tx *sql.Tx) error {
	return setupTransaction(ctx, tx, r.disableFkChecks, r.disableUniqueChecks)
}

func (r *TableDataRestorerCsv) openTx(ctx context.Context, db *sql.DB) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			r.execErr = err
			if err := closeTransaction(ctx, tx, r.execErr, r.disableFkChecks, r.disableUniqueChecks); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("failed to close transaction in defer")
			}
		}
	}()

	if err = r.setupTx(ctx, tx); err != nil {
		return err
	}

	r.tx = tx
	return nil
}

func (r *TableDataRestorerCsv) connectDB(ctx context.Context) (err error) {
	uri, err := r.connConfig.URI()
	if err != nil {
		return fmt.Errorf("get connection URI: %w", err)
	}

	db, err := sql.Open("mysql", uri)
	if err != nil {
		return fmt.Errorf("open mysql connection: %w", err)
	}

	defer func() {
		if err != nil {
			closeDatabase(ctx, db)
		}
	}()

	if err = r.openTx(ctx, db); err != nil {
		return fmt.Errorf("open transaction: %w", err)
	}

	r.db = db
	return nil
}

func (r *TableDataRestorerCsv) Init(ctx context.Context) error {
	file, err := r.st.GetObject(ctx, r.meta.Filename)
	if err != nil {
		return fmt.Errorf("open table data file %s: %w", r.meta.Filename, err)
	}
	var readCloser io.ReadCloser
	readCloser = file
	if r.compress {
		readCloser, err = commonutils.NewGzipReader(file, r.pgzip)
		if err != nil {
			return fmt.Errorf("create gzip reader for file %s: %w", r.meta.Filename, err)
		}
	}

	mysql.RegisterReaderHandler(getFileHandlerName(*r.table), func() io.Reader {
		// You do not need to close the reader, it will be closed automatically	by the driver.
		// It's hard to believe but the driver tries to cast io.Reader to io.ReadCloser
		// and close it
		return readCloser
	})

	return r.connectDB(ctx)
}

func (r *TableDataRestorerCsv) Restore(ctx context.Context) error {
	ctx = log.Ctx(ctx).With().
		Str(models.MetaKeyTableSchema, r.table.Schema).
		Str(models.MetaKeyTableName, r.table.Name).
		Logger().WithContext(ctx)

	if err := r.restoreTable(ctx, r.tx); err != nil {
		r.execErr = err
		return fmt.Errorf("restore table data: %w", err)
	}

	if err := r.showWarnings(ctx, r.db); err != nil {
		return fmt.Errorf("show warnings: %w", err)
	}

	return nil
}

func (r *TableDataRestorerCsv) closeTx(ctx context.Context) error {
	return closeTransaction(ctx, r.tx, r.execErr, r.disableFkChecks, r.disableUniqueChecks)
}

func (r *TableDataRestorerCsv) closeDB(ctx context.Context) {
	closeDatabase(ctx, r.db)
}

func (r *TableDataRestorerCsv) Close(ctx context.Context) error {
	if err := r.closeTx(ctx); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to close transaction")
	}

	r.closeDB(ctx)

	r.taskResolver.SetTaskCompleted(r.meta.TaskID)
	mysql.DeregisterReaderHandler(getFileHandlerName(*r.table))
	return nil
}
