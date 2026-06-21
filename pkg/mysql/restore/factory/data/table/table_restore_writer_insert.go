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

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/huandu/go-sqlbuilder"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/connconfig"
)

var _ core.RestoreRowWriter = (*InsertRestoreWriter)(nil)

const (
	insertSeparatorLen   = 2               // ", " between tuples in the VALUES list
	insertTerminatorLen  = 1               // ";" appended by Build()
	defaultMaxInsertSize = 4 * 1024 * 1024 // 4 MiB default batch size
)

// InsertRestoreWriter is the restore-side symmetric counterpart of TableDataReader.
//
// Open borrows one pooled raw connection for the entire table via
// RunWithEngineResource, starts a single READ WRITE transaction, and receives
// rows from WriteRow through a channel — mirroring the goroutine-channel pattern
// used by TableDataReader on the dump side. The full table is written atomically:
// if any write fails the transaction is rolled back.
type InsertRestoreWriter struct {
	table   *core.Table // copy with RemapDatabase applied at Open time
	opts    TableRestoreOpts
	rowCh   chan []byte
	errorCh chan error // buffered(1): writeLoop → WriteRow/Close
	eg      *errgroup.Group
	cancel  context.CancelFunc
	// headerLen is the byte length of the INSERT header up to "VALUES ",
	// pre-computed once so that batch size estimation is cheap.
	headerLen int
}

func NewInsertRestoreWriter(table *core.Table) *InsertRestoreWriter {
	return &InsertRestoreWriter{table: table}
}

// Open binds the session and connection config, applies RemapDatabase, then
// starts the write-loop goroutine that holds the pooled connection and transaction
// for the lifetime of this table's restore.
func (w *InsertRestoreWriter) Open(
	ctx context.Context,
	session core.DatabaseSession,
	conn core.ConnectionConfigurer,
) error {
	cc, ok := conn.ConnectionConfig().(*connconfig.RestoreConnectionConfig)
	if !ok {
		return fmt.Errorf("insert writer: expected *connconfig.RestoreConnectionConfig, got %T", conn.ConnectionConfig())
	}
	w.opts = cc.TableRestoreOptions()

	// Work on a private copy so RemapDatabase does not mutate the shared table.
	w.applyTableRemap()

	w.headerLen = w.computeHeaderLen()
	w.rowCh = make(chan []byte)
	w.errorCh = make(chan error, 1)

	var egCtx context.Context
	ctx, w.cancel = context.WithCancel(ctx)
	w.eg, egCtx = errgroup.WithContext(ctx)

	w.eg.Go(func() error {
		err := session.RunWithEngineResource(egCtx, func(ctx context.Context, res any) error {
			wc, ok := res.(pool.WorkerConn)
			if !ok {
				return fmt.Errorf("insert writer: expected pool.WorkerConn, got %T", res)
			}
			return w.writeLoop(ctx, wc.RawConn())
		})
		if err != nil {
			// Non-blocking: errorCh is buffered(1) and the goroutine sends at most once.
			w.errorCh <- err
		}
		return err
	})
	return nil
}

// WriteRow sends a raw values tuple (e.g. `(1,'foo',NULL)`) to the write loop.
// It blocks until the loop accepts the row, the loop fails, or ctx is cancelled.
func (w *InsertRestoreWriter) WriteRow(ctx context.Context, row []byte) error {
	select {
	case w.rowCh <- row:
		return nil
	case err := <-w.errorCh:
		return fmt.Errorf("insert writer: write loop failed: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("insert writer: %w", ctx.Err())
	}
}

// Close signals the write loop that all rows have been sent and waits for it to
// flush the remaining batch and commit the transaction.
func (w *InsertRestoreWriter) Close(_ context.Context) error {
	if w.cancel != nil {
		defer w.cancel()
	}
	if w.rowCh != nil {
		close(w.rowCh)
	}
	if w.eg == nil {
		return nil
	}
	if err := w.eg.Wait(); err != nil {
		return fmt.Errorf("close insert writer: %w", err)
	}
	return nil
}

// applyTableRemap replaces w.table with a copy whose Schema is remapped via
// opts.RemapDatabase. Called from Open so the shared table pointer is not mutated.
func (w *InsertRestoreWriter) applyTableRemap() {
	tableCopy := *w.table
	if mapped, ok := w.opts.RemapDatabase[tableCopy.Schema]; ok {
		tableCopy.Schema = mapped
	}
	w.table = &tableCopy
}

// writeLoop runs inside the RunWithEngineResource callback. It owns the
// connection and transaction for the full table restore.
func (w *InsertRestoreWriter) writeLoop(ctx context.Context, conn *client.Conn) error {
	if w.opts.DisableForeignKeyChecks {
		if _, err := conn.Execute("SET FOREIGN_KEY_CHECKS=0"); err != nil {
			return fmt.Errorf("disable foreign key checks: %w", err)
		}
	}
	if w.opts.DisableUniqueChecks {
		if _, err := conn.Execute("SET UNIQUE_CHECKS=0"); err != nil {
			return fmt.Errorf("disable unique checks: %w", err)
		}
	}

	// START TRANSACTION READ WRITE transitions the pooled connection from the
	// snapshot READ ONLY transaction (used for dump consistency) to a writable
	// transaction. This is intentional: the dump pool uses REPEATABLE READ
	// snapshot transactions; for restore we need write access.
	if _, err := conn.Execute("START TRANSACTION READ WRITE"); err != nil {
		return fmt.Errorf("start transaction: %w", err)
	}

	var (
		batch         [][]byte
		batchSize     int
		batchNum      int
		totalWarnings int
		printedCount  int
	)

	maxSize := w.opts.MaxInsertStatementSize
	if maxSize <= 0 {
		maxSize = defaultMaxInsertSize
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		batchNum++
		stmt := w.buildBatch(batch)
		res, err := conn.Execute(stmt)
		if err != nil {
			return fmt.Errorf("execute insert batch %d: %w", batchNum, err)
		}
		count, warnErr := w.showWarnings(ctx, conn, int(res.Warnings), batchNum, &printedCount)
		if warnErr != nil {
			log.Ctx(ctx).Warn().Err(warnErr).Msg("failed to show insert warnings")
		}
		totalWarnings += count
		batch = batch[:0]
		batchSize = 0
		return nil
	}

	for {
		select {
		case row, ok := <-w.rowCh:
			if !ok {
				// Channel closed: flush remaining batch and commit.
				if err := flush(); err != nil {
					if _, rb := conn.Execute("ROLLBACK"); rb != nil {
						log.Ctx(ctx).Error().Err(rb).Msg("rollback failed after flush error")
					}
					return err
				}
				if w.opts.DisableUniqueChecks {
					if _, err := conn.Execute("SET UNIQUE_CHECKS=1"); err != nil {
						log.Ctx(ctx).Error().Err(err).Msg("re-enable unique checks")
					}
				}
				if w.opts.DisableForeignKeyChecks {
					if _, err := conn.Execute("SET FOREIGN_KEY_CHECKS=1"); err != nil {
						log.Ctx(ctx).Error().Err(err).Msg("re-enable foreign key checks")
					}
				}
				if _, err := conn.Execute("COMMIT"); err != nil {
					if _, rb := conn.Execute("ROLLBACK"); rb != nil {
						log.Ctx(ctx).Error().Err(rb).Msg("rollback failed after commit error")
					}
					return fmt.Errorf("commit transaction: %w", err)
				}
				if totalWarnings > 0 {
					log.Ctx(ctx).Warn().
						Int("totalWarnings", totalWarnings).
						Str("table", w.table.Schema+"."+w.table.Name).
						Msg("warnings occurred during table restore")
				}
				return nil
			}

			// Accumulate row; flush before adding if it would exceed max size.
			tupleLen := len(row)
			var newSize int
			if len(batch) == 0 {
				newSize = w.headerLen + tupleLen + insertTerminatorLen
			} else {
				newSize = batchSize + insertSeparatorLen + tupleLen
			}
			if len(batch) > 0 && newSize > maxSize {
				if err := flush(); err != nil {
					if _, rb := conn.Execute("ROLLBACK"); rb != nil {
						log.Ctx(ctx).Error().Err(rb).Msg("rollback failed after flush error")
					}
					return err
				}
				newSize = w.headerLen + tupleLen + insertTerminatorLen
			}
			tuple := make([]byte, len(row))
			copy(tuple, row)
			batch = append(batch, tuple)
			batchSize = newSize

		case <-ctx.Done():
			if _, err := conn.Execute("ROLLBACK"); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("rollback on context cancellation")
			}
			return fmt.Errorf("insert writer: %w", ctx.Err())
		}
	}
}

// newInsertBuilder creates a fresh InsertBuilder for this table configured with
// the correct verb (INSERT / INSERT IGNORE / REPLACE INTO).
func (w *InsertRestoreWriter) newInsertBuilder() *sqlbuilder.InsertBuilder {
	ib := sqlbuilder.MySQL.NewInsertBuilder()
	tableName := sqlbuilder.MySQL.Quote(w.table.Schema) + "." + sqlbuilder.MySQL.Quote(w.table.Name)
	switch {
	case w.opts.InsertReplace:
		ib.ReplaceInto(tableName)
	case w.opts.InsertIgnore:
		ib.InsertIgnoreInto(tableName)
	default:
		ib.InsertInto(tableName)
	}
	cols := make([]string, len(w.table.Columns))
	for i, col := range w.table.Columns {
		cols[i] = sqlbuilder.MySQL.Quote(col.Name)
	}
	ib.Cols(cols...)
	return ib
}

// computeHeaderLen returns the byte length of the INSERT header through "VALUES "
// so that batch size estimation can skip rebuilding the header each time.
func (w *InsertRestoreWriter) computeHeaderLen() int {
	ib := w.newInsertBuilder()
	ib.Values(sqlbuilder.Raw("X"))
	stmt, _ := ib.Build()
	idx := strings.LastIndex(stmt, "VALUES ")
	if idx < 0 {
		return len(stmt)
	}
	return idx + len("VALUES ")
}

// buildBatch assembles one INSERT statement from the accumulated tuple bytes.
// Each tuple is the raw bytes from the dump file including outer parens, e.g.
// `(1,'foo',NULL)`. The outer parens are stripped before passing to Values() so
// that sqlbuilder re-wraps them correctly.
func (w *InsertRestoreWriter) buildBatch(tuples [][]byte) string {
	ib := w.newInsertBuilder()
	for _, tuple := range tuples {
		inner := tuple[1 : len(tuple)-1]
		ib.Values(sqlbuilder.Raw(string(inner)))
	}
	stmt, _ := ib.Build()
	return stmt
}

// showWarnings logs any MySQL warnings that followed the most recent INSERT batch.
// warningCount is taken from res.Warnings (the server's warning counter for the
// last statement) to avoid an extra round-trip for the common zero-warnings case.
func (w *InsertRestoreWriter) showWarnings(
	ctx context.Context,
	conn *client.Conn,
	warningCount int,
	batchNum int,
	printedCount *int,
) (int, error) {
	if warningCount == 0 || !w.opts.PrintWarnings {
		return warningCount, nil
	}

	maxFetch := w.opts.MaxFetchWarnings
	var fetchLimit int
	if maxFetch > 0 {
		fetchLimit = maxFetch - *printedCount
		if fetchLimit <= 0 {
			return warningCount, nil
		}
	}

	var query string
	if fetchLimit > 0 {
		query = fmt.Sprintf("SHOW WARNINGS LIMIT %d;", fetchLimit)
	} else {
		query = "SHOW WARNINGS;"
	}

	result, err := conn.Execute(query)
	if err != nil {
		return 0, fmt.Errorf("fetch warnings: %w", err)
	}

	for _, row := range result.Values {
		if len(row) < 3 {
			continue
		}
		log.Ctx(ctx).Warn().
			Str("MysqlLevel", string(row[0].AsString())).
			Str("MysqlCode", string(row[1].AsString())).
			Str("MysqlWarning", string(row[2].AsString())).
			Int("BatchNum", batchNum).
			Msg("warning from MySQL server during table restore")
		*printedCount++
	}

	return warningCount, nil
}
