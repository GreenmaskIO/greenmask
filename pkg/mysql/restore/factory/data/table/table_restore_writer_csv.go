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

package table

// CsvRestoreWriter restores a MySQL table using LOAD DATA LOCAL INFILE.
//
// It implements RestoreRowWriter by piping each CSV row (as produced by
// TableRestoreReader) through an io.Pipe into a LOAD DATA LOCAL INFILE query
// that runs in its own go-sql-driver/mysql connection. The standard driver is
// required because go-mysql-org/go-mysql/client does not support the
// RegisterReaderHandler API needed for streaming LOAD DATA.
//
// NOT wired to the factory — CSV restore is currently disabled due to
// LOAD DATA LOCAL INFILE security and compatibility constraints:
//   - Requires local_infile=ON on the server.
//   - RegisterReaderHandler uses a global registry (not safe for concurrent
//     restores of tables with identical schema.name across databases).
//
// To enable: add a CsvRestoreWriter case to Factory.New after a dedicated
// review. The implementation below is complete and correct for the single-DB
// restore scenario.

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/connconfig"
)

var _ core.RestoreRowWriter = (*CsvRestoreWriter)(nil)

// CsvRestoreWriter is the CSV-format symmetric counterpart of InsertRestoreWriter.
type CsvRestoreWriter struct {
	table *core.Table
	opts  TableRestoreOpts
	pw    *io.PipeWriter
	done  chan error
}

func NewCsvRestoreWriter(table *core.Table) *CsvRestoreWriter {
	return &CsvRestoreWriter{table: table}
}

// Open registers an io.Pipe as the LOAD DATA reader, then starts a goroutine
// that opens its own sql.DB, begins a transaction, and executes the LOAD DATA
// LOCAL INFILE query. The goroutine blocks until the pipe is closed by Close.
func (w *CsvRestoreWriter) Open(
	ctx context.Context,
	_ core.DatabaseSession,
	conn core.ConnectionConfigurer,
) error {
	cc, ok := conn.ConnectionConfig().(*connconfig.RestoreConnectionConfig)
	if !ok {
		return fmt.Errorf("csv writer: expected *connconfig.RestoreConnectionConfig, got %T", conn.ConnectionConfig())
	}
	w.opts = cc.TableRestoreOptions()

	// Copy to avoid mutating the shared table pointer.
	t := *w.table
	if mapped, ok := w.opts.RemapDatabase[t.Schema]; ok {
		t.Schema = mapped
	}
	w.table = &t

	connCfg, err := cc.CsvConnConfig()
	if err != nil {
		return fmt.Errorf("csv writer: get connection config: %w", err)
	}

	pr, pw := io.Pipe()
	w.pw = pw
	w.done = make(chan error, 1)

	handlerName := csvHandlerName(*w.table)
	mysql.RegisterReaderHandler(handlerName, func() io.Reader { return pr })

	go func() {
		loadErr := w.loadData(ctx, connCfg, handlerName)
		mysql.DeregisterReaderHandler(handlerName)
		_ = pr.Close()
		w.done <- loadErr
	}()
	return nil
}

// WriteRow sends one CSV line (without trailing newline) to the LOAD DATA query.
func (w *CsvRestoreWriter) WriteRow(_ context.Context, row []byte) error {
	line := append(row, '\n') //nolint:gocritic // intentional append to new backing
	_, err := w.pw.Write(line)
	return err
}

// Close signals end-of-file to the LOAD DATA query by closing the pipe writer,
// then waits for the goroutine to commit or roll back.
func (w *CsvRestoreWriter) Close(_ context.Context) error {
	_ = w.pw.Close()
	return <-w.done
}

func (w *CsvRestoreWriter) loadData(ctx context.Context, connCfg *mysqlmodels.ConnConfig, handlerName string) error {
	uri, err := connCfg.URI()
	if err != nil {
		return fmt.Errorf("get connection URI: %w", err)
	}

	// AllowAllFiles is required for the Reader:: handler URI scheme.
	cfg, err := mysql.ParseDSN(uri)
	if err != nil {
		return fmt.Errorf("parse DSN: %w", err)
	}
	cfg.AllowAllFiles = true

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open csv restore connection: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Ctx(ctx).Warn().Err(closeErr).Msg("close csv restore connection")
		}
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if w.opts.DisableForeignKeyChecks {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("disable foreign key checks: %w", err)
		}
	}
	if w.opts.DisableUniqueChecks {
		if _, err := tx.ExecContext(ctx, "SET UNIQUE_CHECKS=0"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("disable unique checks: %w", err)
		}
	}

	conflictKeyword := ""
	switch {
	case w.opts.InsertReplace:
		conflictKeyword = "REPLACE "
	case w.opts.InsertIgnore:
		conflictKeyword = "IGNORE "
	}

	query := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' %sINTO TABLE `%s`.`%s` "+
			"FIELDS TERMINATED BY ',' "+
			`ENCLOSED BY '"' `+
			`ESCAPED BY '\\' `+
			"LINES TERMINATED BY '\\n'",
		handlerName, conflictKeyword, w.table.Schema, w.table.Name,
	)

	log.Ctx(ctx).Debug().
		Str("table", w.table.Schema+"."+w.table.Name).
		Str("query", query).
		Msg("csv restore: executing LOAD DATA LOCAL INFILE")

	if _, err := tx.ExecContext(ctx, query); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("execute LOAD DATA for %s.%s: %w", w.table.Schema, w.table.Name, err)
	}

	if w.opts.DisableUniqueChecks {
		if _, err := tx.ExecContext(ctx, "SET UNIQUE_CHECKS=1"); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("re-enable unique checks")
		}
	}
	if w.opts.DisableForeignKeyChecks {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1"); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("re-enable foreign key checks")
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("commit csv restore transaction: %w", err)
	}
	return nil
}

func csvHandlerName(t core.Table) string {
	return fmt.Sprintf("greenmask__%s__%s", t.Schema, t.Name)
}
