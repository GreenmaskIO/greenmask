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

package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

var _ core.RestoreConn = (*restoreWorkerConn)(nil)

// restoreWorkerConn is a single pooled *sql.Conn for restore operations.
// It satisfies core.RestoreConn (ID/DB). Transaction control is exposed only as
// unexported helpers used by the RestoreSession implementations — restorers never
// drive the transaction themselves.
type restoreWorkerConn struct {
	id   int
	conn *sql.Conn
	tx   *sql.Tx
}

func (r *restoreWorkerConn) ID() int { return r.id }

func (r *restoreWorkerConn) DB() core.DB {
	if r.tx != nil {
		return r.tx
	}
	return r.conn
}

// begin opens a transaction on the underlying connection and makes DB() return it.
func (r *restoreWorkerConn) begin(ctx context.Context) error {
	tx, err := r.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx on restore conn %d: %w", r.id, err)
	}
	r.tx = tx
	return nil
}

// commit commits the active transaction, if any. A no-op when none is open.
func (r *restoreWorkerConn) commit() error {
	if r.tx == nil {
		return nil
	}
	err := r.tx.Commit()
	r.tx = nil
	if err != nil {
		return fmt.Errorf("commit tx on restore conn %d: %w", r.id, err)
	}
	return nil
}

// rollback rolls back the active transaction, if any. A no-op when none is open.
func (r *restoreWorkerConn) rollback() error {
	if r.tx == nil {
		return nil
	}
	err := r.tx.Rollback()
	r.tx = nil
	if err != nil {
		return fmt.Errorf("rollback tx on restore conn %d: %w", r.id, err)
	}
	return nil
}

// restoreConnPool is the shared base for the restore session implementations. It
// owns all connection plumbing — opening the *sql.DB, building the worker conns,
// the borrow/return queue, and Close — leaving only the transactional lifecycle
// to the concrete session types.
type restoreConnPool struct {
	cfg      *mysqlmodels.ConnConfig
	db       *sql.DB
	pool     []*restoreWorkerConn
	queue    chan *restoreWorkerConn
	poolSize int
}

func newRestoreConnPool(cfg *mysqlmodels.ConnConfig, poolSize int) restoreConnPool {
	return restoreConnPool{
		cfg:      cfg,
		poolSize: poolSize,
	}
}

// initConns opens the database and poolSize dedicated connections. When beginTx
// is true a transaction is started on each connection (single-tx mode). On any
// error the partially-opened pool is closed before returning.
//
// It is idempotent: once the connection queue exists the session is already
// initialized and a repeat call is a no-op. This lets the pipeline runtime open
// the session eagerly (for pre-restore introspection) while the processor still
// calls Init defensively.
func (p *restoreConnPool) initConns(ctx context.Context, beginTx bool) (err error) {
	if p.queue != nil {
		return nil
	}
	defer func() {
		if err != nil {
			if closeErr := p.Close(context.Background()); closeErr != nil {
				log.Ctx(ctx).Warn().Err(closeErr).Msg("failed to close restore pool after initialization error")
			}
		}
	}()

	uri, err := p.cfg.URI()
	if err != nil {
		return fmt.Errorf("get restore connection URI: %w", err)
	}
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return fmt.Errorf("open restore mysql db: %w", err)
	}
	p.db = db

	p.pool = make([]*restoreWorkerConn, p.poolSize)
	for i := 0; i < p.poolSize; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("open restore sql conn %d: %w", i, err)
		}
		wc := &restoreWorkerConn{id: i, conn: conn}
		if beginTx {
			if err := wc.begin(ctx); err != nil {
				if cerr := conn.Close(); cerr != nil {
					log.Ctx(ctx).Warn().Err(cerr).Int("connID", i).Msg("close restore conn after begin tx failure")
				}
				return fmt.Errorf("begin single tx on restore conn %d: %w", i, err)
			}
		}
		p.pool[i] = wc
		log.Ctx(ctx).Debug().Int("connID", i).Msg("opened restore sql mysql connection")
	}

	p.queue = make(chan *restoreWorkerConn, p.poolSize)
	for _, wc := range p.pool {
		p.queue <- wc
	}
	return nil
}

// RunWithOperationalDB is not supported by restore sessions.
// Use RunWithEngineResource with a core.RestoreConn instead.
func (p *restoreConnPool) RunWithOperationalDB(_ context.Context, _ func(ctx context.Context, db core.DB) error) error {
	return core.ErrEngineResourceNotSupported
}

// withConn borrows a worker conn for the duration of fn and always returns it
// afterwards — even on error or panic.
func (p *restoreConnPool) withConn(ctx context.Context, fn func(ctx context.Context, rc *restoreWorkerConn) error) error {
	if p.queue == nil {
		return fmt.Errorf("restore pool: not initialised")
	}
	var rc *restoreWorkerConn
	select {
	case rc = <-p.queue:
		log.Ctx(ctx).Debug().Int("connID", rc.ID()).Msg("acquired restore conn from pool")
	case <-ctx.Done():
		return fmt.Errorf("acquire restore conn: %w", ctx.Err())
	}
	defer func() {
		p.queue <- rc
		log.Ctx(ctx).Debug().Int("connID", rc.ID()).Msg("returned restore conn to pool")
	}()
	return fn(ctx, rc)
}

// eachConn applies fn to every worker conn (used by DoneWithError to commit or
// roll back the per-conn transactions), joining any errors.
func (p *restoreConnPool) eachConn(fn func(rc *restoreWorkerConn) error) error {
	var errs []error
	for _, wc := range p.pool {
		if wc == nil {
			continue
		}
		if err := fn(wc); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *restoreConnPool) Close(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		logger := log.Ctx(ctx)
		bgCtx := logger.WithContext(context.Background())
		done <- p.close(bgCtx)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *restoreConnPool) close(ctx context.Context) error {
	var errs []error

	if p.pool != nil {
		connErrs := make([]error, len(p.pool))
		var wg sync.WaitGroup
		for i, wc := range p.pool {
			wg.Go(func() {
				connErrs[i] = p.closeWorkerConn(ctx, wc)
			})
		}
		wg.Wait()
		errs = append(errs, connErrs...)
	}

	if p.db != nil {
		if err := p.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close restore db: %w", err))
		}
	}

	return errors.Join(errs...)
}

// closeWorkerConn releases a single connection's resources. Any transaction still
// open at Close time (e.g. an un-finalized session after an Init failure) is rolled
// back best-effort so the connection can be returned cleanly to the driver.
func (p *restoreConnPool) closeWorkerConn(ctx context.Context, wc *restoreWorkerConn) error {
	if wc == nil || wc.conn == nil {
		return nil
	}
	if wc.tx != nil {
		if err := wc.rollback(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Int("connID", wc.id).Msg("rollback lingering tx on restore conn close")
		}
	}
	if err := wc.conn.Close(); err != nil {
		return fmt.Errorf("close restore sql conn %d: %w", wc.id, err)
	}
	log.Ctx(ctx).Debug().Int("connID", wc.id).Msg("closed restore sql connection in pool")
	return nil
}
