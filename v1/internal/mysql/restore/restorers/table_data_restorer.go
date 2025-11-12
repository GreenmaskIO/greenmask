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

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/config"
)

const dumperTypeTableData = "table_restorer"

var (
	_ commonininterfaces.Restorer = (*TableDataRestorer)(nil)
)

type TableDataRestorer struct {
	table        *commonmodels.Table
	meta         commonmodels.RestorationItem
	connConfig   config.ConnectionOpts
	st           commonininterfaces.Storager
	taskResolver commonininterfaces.TaskMapper
	compress     bool
	pgzip        bool
}

func (r *TableDataRestorer) Meta() map[string]any {
	return map[string]any{
		commonmodels.MetaKeyTableSchema:      r.table.Schema,
		commonmodels.MetaKeyTableName:        r.table.Name,
		commonmodels.MetaKeyUniqueDumpTaskID: r.DebugInfo(),
	}
}

func (r *TableDataRestorer) DebugInfo() string {
	return utils.GetUniqueTaskID(dumperTypeTableData, r.table.Schema, r.table.Name)
}

type Option func(v *TableDataRestorer) error

func WithCompression(
	enabled bool,
	pgzip bool,
) Option {
	return func(v *TableDataRestorer) error {
		v.compress = enabled
		v.pgzip = pgzip
		return nil
	}
}

func NewTableDataRestorer(
	meta commonmodels.RestorationItem,
	connConfig config.ConnectionOpts,
	st commonininterfaces.Storager,
	taskResolver commonininterfaces.TaskMapper,
	opts ...Option,
) (*TableDataRestorer, error) {
	var table commonmodels.Table
	if err := json.Unmarshal(meta.ObjectDefinition, &table); err != nil {
		return nil, err
	}
	if err := table.Validate(); err != nil {
		return nil, fmt.Errorf("validate table: %w", err)
	}
	res := &TableDataRestorer{
		table:        &table,
		meta:         meta,
		connConfig:   connConfig,
		st:           st,
		taskResolver: taskResolver,
	}
	for _, opt := range opts {
		if err := opt(res); err != nil {
			return nil, fmt.Errorf("options failed: %w", err)
		}
	}
	return res, nil
}

func getFileHandlerName(t commonmodels.Table) string {
	return fmt.Sprintf("%s__%s", t.Schema, t.Name)
}

func (r *TableDataRestorer) showWarnings(ctx context.Context, db *sql.DB) error {
	rows, err := db.Query("SHOW WARNINGS;")
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to close rows")
		}
	}()

	for rows.Next() {
		var level, code, message string
		if err := rows.Scan(&level, &code, &message); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		log.Ctx(ctx).Warn().
			Str("MysqlLevel", level).
			Str("MysqlCode", code).
			Str("MysqlWarning", message).
			Msg("warning from Mysql server after restoring table data")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}
	return nil
}

func (r *TableDataRestorer) restoreTable(ctx context.Context, db *sql.DB) error {
	// TODO: REPLACE option
	// YOU MIGHT WANT TO USE LOAD DATA LOCAL INFILE 'Reader::%s' REPLACE
	// I think you should implement a replace option in the config.
	query := fmt.Sprintf(
		`LOAD DATA LOCAL INFILE 'Reader::%s' `+
			"IGNORE INTO TABLE `%s`.`%s` "+
			`FIELDS TERMINATED BY ',' `+
			`ENCLOSED BY '"' `+
			`ESCAPED BY '"' `+
			`LINES TERMINATED BY '\n'`,
		getFileHandlerName(*r.table),
		r.table.Schema,
		r.table.Name,
	)

	log.Ctx(ctx).Debug().
		Str(commonmodels.MetaKeyQuery, query).
		Msg("restoring table data")

	res, err := db.Exec(query)
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

func (r *TableDataRestorer) Init(ctx context.Context) error {
	file, err := r.st.GetObject(ctx, r.meta.Filename)
	if err != nil {
		return fmt.Errorf("open table data file %s: %w", r.meta.Filename, err)
	}
	var readCloser io.ReadCloser
	readCloser = file
	if r.compress {
		readCloser, err = utils.NewGzipReader(file, false)
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
	return nil
}

// setupConnection - set up the MySQL connection to disable or enable some settings on the session level.
func (r *TableDataRestorer) setupConnection(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1;")
	if err != nil {
		return fmt.Errorf("disable foreign key checks: %w", err)
	}
	return nil
}

func (r *TableDataRestorer) Restore(ctx context.Context) error {
	ctx = log.Ctx(ctx).With().
		Str(commonmodels.MetaKeyTableSchema, r.table.Schema).
		Str(commonmodels.MetaKeyTableName, r.table.Name).
		Logger().WithContext(ctx)

	connCfg, err := r.connConfig.ConnectionConfig()
	if err != nil {
		return fmt.Errorf("get connection config: %w", err)
	}
	uri, err := connCfg.URI()
	if err != nil {
		return fmt.Errorf("get connection URI: %w", err)
	}
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return fmt.Errorf("open mysql connection: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Ctx(ctx).Error().Err(closeErr).Msg("failed to close database connection")
		}
	}()

	//if err := r.setupConnection(ctx, db); err != nil {
	//	return fmt.Errorf("setup connection: %w", err)
	//}

	if err := r.restoreTable(ctx, db); err != nil {
		return fmt.Errorf("restore table data: %w", err)
	}

	if err := r.showWarnings(ctx, db); err != nil {
		return fmt.Errorf("show warnings: %w", err)
	}

	return nil
}

func (r *TableDataRestorer) Close(_ context.Context) error {
	r.taskResolver.SetTaskCompleted(r.meta.TaskID)
	mysql.DeregisterReaderHandler(getFileHandlerName(*r.table))
	return nil
}
