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
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	alocutils "github.com/go-mysql-org/go-mysql/utils"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/mysql/config"
)

type TableDataRestorerInsert struct {
	table               *models.Table
	meta                models.RestorationItem
	connConfig          config.ConnectionOpts
	st                  interfaces.Storager
	taskResolver        interfaces.TaskMapper
	compress            bool
	pgzip               bool
	printWarnings       bool
	maxFetchWarnings    int
	disableFkChecks     bool
	disableUniqueChecks bool
	reader              io.ReadCloser
	totalWarnings       int
	printedCount        int
	db                  *sql.DB
	tx                  *sql.Tx
	execErr             error
}

func NewTableDataRestorerInsert(
	meta models.RestorationItem,
	connConfig config.ConnectionOpts,
	st interfaces.Storager,
	taskResolver interfaces.TaskMapper,
	opts ...Option,
) (*TableDataRestorerInsert, error) {
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

	res := &TableDataRestorerInsert{
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
	}

	return res, nil
}

func (r *TableDataRestorerInsert) Meta() map[string]any {
	return map[string]any{
		models.MetaKeyTableSchema:      r.table.Schema,
		models.MetaKeyTableName:        r.table.Name,
		models.MetaKeyUniqueDumpTaskID: r.DebugInfo(),
	}
}

func (r *TableDataRestorerInsert) DebugInfo() string {
	return utils.GetUniqueTaskID(dumperTypeTableData, r.table.Schema, r.table.Name)
}

func (r *TableDataRestorerInsert) setupTx(ctx context.Context, tx *sql.Tx) error {
	return setupTransaction(ctx, tx, r.disableFkChecks, r.disableUniqueChecks)
}

func (r *TableDataRestorerInsert) openTx(ctx context.Context, db *sql.DB) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			r.execErr = err
			_ = closeTransaction(ctx, tx, r.execErr, r.disableFkChecks, r.disableUniqueChecks)
		}
	}()

	if err = r.setupTx(ctx, tx); err != nil {
		return err
	}

	r.tx = tx
	return nil
}

func (r *TableDataRestorerInsert) connectDB(ctx context.Context) (err error) {
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

func (r *TableDataRestorerInsert) Init(ctx context.Context) error {
	file, err := r.st.GetObject(ctx, r.meta.Filename)
	if err != nil {
		return fmt.Errorf("open table data file %s: %w", r.meta.Filename, err)
	}

	var readCloser io.ReadCloser
	readCloser = file
	if r.compress {
		readCloser, err = utils.NewGzipReader(file, r.pgzip)
		if err != nil {
			return fmt.Errorf("create gzip reader for file %s: %w", r.meta.Filename, err)
		}
	}
	r.reader = readCloser

	return r.connectDB(ctx)
}

func (r *TableDataRestorerInsert) Restore(ctx context.Context) error {
	ctx = log.Ctx(ctx).With().
		Str(models.MetaKeyTableSchema, r.table.Schema).
		Str(models.MetaKeyTableName, r.table.Name).
		Logger().WithContext(ctx)

	if err := r.restoreTable(ctx); err != nil {
		r.execErr = err
		return fmt.Errorf("restore table data: %w", err)
	}

	if r.totalWarnings > 0 {
		log.Ctx(ctx).Warn().
			Int("TotalWarnings", r.totalWarnings).
			Msg("warnings occurred during table data restoration")
		if r.printWarnings && r.maxFetchWarnings > 0 && r.totalWarnings > r.printedCount {
			log.Ctx(ctx).Warn().
				Int("SuppressedCount", r.totalWarnings-r.printedCount).
				Msg("more warnings suppressed")
		}
	}

	return nil
}

func (r *TableDataRestorerInsert) restoreTable(ctx context.Context) error {
	reader := bufio.NewReader(r.reader)
	var stmt []byte
	var batchNum int

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("read sql content at batch %d: %w", batchNum+1, err)
		}

		if len(line) > 0 {
			stmt = append(stmt, line...)
		}

		// Check if we reached the end of an INSERT statement.
		// We trim trailing whitespace (including \r and \n) to be robust against CRLF
		// and different formatting.
		trimmed := bytes.TrimSpace(line)
		if bytes.HasSuffix(trimmed, []byte(";")) || (err == io.EOF && len(stmt) > 0) {
			batchNum++

			stmtStr := alocutils.ByteSliceToString(stmt)

			if strings.TrimSpace(stmtStr) != "" {
				_, execErr := r.tx.ExecContext(ctx, stmtStr)
				if execErr != nil {
					return fmt.Errorf("execute batch %d: %w", batchNum, execErr)
				}
				count, err := showInsertWarnings(ctx, r.db, r.printWarnings, r.maxFetchWarnings, batchNum, &r.printedCount)
				if err != nil {
					log.Ctx(ctx).Warn().Err(err).Msg("failed to show warnings after batch")
				}
				r.totalWarnings += count
			}
			stmt = stmt[:0] // reset without reallocating
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

// showWarnings is now handled by showInsertWarnings in restoreTable

func (r *TableDataRestorerInsert) closeTx(ctx context.Context) error {
	return closeTransaction(ctx, r.tx, r.execErr, r.disableFkChecks, r.disableUniqueChecks)
}

func (r *TableDataRestorerInsert) closeDB(ctx context.Context) {
	closeDatabase(ctx, r.db)
}

func (r *TableDataRestorerInsert) Close(ctx context.Context) error {
	if err := r.closeTx(ctx); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to close transaction")
	}

	r.closeDB(ctx)

	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("close mysql reader")
		}
	}
	r.taskResolver.SetTaskCompleted(r.meta.TaskID)
	return nil
}
