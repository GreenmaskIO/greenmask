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

	"github.com/huandu/go-sqlbuilder"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/mysql/config"
)

type TableDataRestorerInsert struct {
	table                  *models.Table
	meta                   models.RestorationItem
	connConfig             config.ConnectionOpts
	st                     interfaces.Storager
	taskResolver           interfaces.TaskMapper
	compress               bool
	pgzip                  bool
	printWarnings          bool
	maxFetchWarnings       int
	disableFkChecks        bool
	disableUniqueChecks    bool
	insertIgnore           bool
	insertReplace          bool
	maxInsertStatementSize int
	// headerLen is the byte length of the INSERT header including "VALUES ",
	// pre-computed once so that batch size estimation avoids rebuilding the header.
	headerLen     int
	reader        io.ReadCloser
	totalWarnings int
	printedCount  int
	db            *sql.DB
	tx            *sql.Tx
	execErr       error
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
		table:                  &table,
		meta:                   meta,
		connConfig:             connConfig,
		st:                     st,
		taskResolver:           taskResolver,
		compress:               cfg.Compress,
		pgzip:                  cfg.Pgzip,
		printWarnings:          cfg.PrintWarnings,
		maxFetchWarnings:       cfg.MaxFetchWarnings,
		disableFkChecks:        cfg.DisableForeignKeyChecks,
		disableUniqueChecks:    cfg.DisableUniqueChecks,
		insertIgnore:           cfg.InsertIgnore,
		insertReplace:          cfg.InsertReplace,
		maxInsertStatementSize: cfg.MaxInsertStatementSize,
	}
	res.headerLen = res.computeHeaderLen()

	return res, nil
}

// newInsertBuilder creates a fresh InsertBuilder configured with the correct
// verb (INSERT / INSERT IGNORE / REPLACE), table name, and column list.
// The table name and column names are quoted for MySQL using backticks.
func (r *TableDataRestorerInsert) newInsertBuilder() *sqlbuilder.InsertBuilder {
	ib := sqlbuilder.MySQL.NewInsertBuilder()
	tableName := sqlbuilder.MySQL.Quote(r.table.Schema) + "." + sqlbuilder.MySQL.Quote(r.table.Name)

	switch {
	case r.insertReplace:
		ib.ReplaceInto(tableName)
	case r.insertIgnore:
		ib.InsertIgnoreInto(tableName)
	default:
		ib.InsertInto(tableName)
	}

	cols := make([]string, len(r.table.Columns))
	for i, col := range r.table.Columns {
		cols[i] = sqlbuilder.MySQL.Quote(col.Name)
	}
	ib.Cols(cols...)
	return ib
}

// computeHeaderLen returns the byte length of the INSERT header string up to
// and including "VALUES ", by building a single-row dummy statement and
// finding where the first tuple starts.
func (r *TableDataRestorerInsert) computeHeaderLen() int {
	ib := r.newInsertBuilder()
	ib.Values(sqlbuilder.Raw("X"))
	stmt, _ := ib.Build()
	idx := strings.LastIndex(stmt, "VALUES ")
	if idx < 0 {
		return len(stmt)
	}
	return idx + len("VALUES ")
}

// buildBatch builds and executes one INSERT statement from the accumulated tuples.
// Each element of tuples is a raw line from the dump file, e.g. `('val1', 'val2')`.
func (r *TableDataRestorerInsert) buildBatch(tuples [][]byte) string {
	ib := r.newInsertBuilder()
	for _, tuple := range tuples {
		// Each line from the file is a complete tuple including outer parens:
		// ('val1', 'val2'). Strip the outer parens so that Values() re-wraps them.
		inner := tuple[1 : len(tuple)-1]
		ib.Values(sqlbuilder.Raw(string(inner)))
	}
	stmt, _ := ib.Build()
	return stmt
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
	scanner := bufio.NewScanner(r.reader)

	var batch [][]byte
	batchSize := 0
	batchNum := 0
	const separatorLen = 2  // ", " between tuples in the VALUES list
	const terminatorLen = 1 // ";" appended by Build()

	maxSize := r.maxInsertStatementSize
	if maxSize <= 0 {
		maxSize = 4 * 1024 * 1024
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		batchNum++

		stmt := r.buildBatch(batch)
		if _, err := r.tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute batch %d: %w", batchNum, err)
		}
		count, err := showInsertWarnings(ctx, r.db, r.printWarnings, r.maxFetchWarnings, batchNum, &r.printedCount)
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("failed to show warnings after batch")
		}
		r.totalWarnings += count

		batch = batch[:0]
		batchSize = 0
		return nil
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		tuple := make([]byte, len(line))
		copy(tuple, line)
		tupleLen := len(tuple)

		var newSize int
		if len(batch) == 0 {
			newSize = r.headerLen + tupleLen + terminatorLen
		} else {
			newSize = batchSize + separatorLen + tupleLen
		}

		if len(batch) > 0 && newSize > maxSize {
			if err := flush(); err != nil {
				return err
			}
			newSize = r.headerLen + tupleLen + terminatorLen
		}

		batch = append(batch, tuple)
		batchSize = newSize
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan file content: %w", err)
	}

	return flush()
}

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
