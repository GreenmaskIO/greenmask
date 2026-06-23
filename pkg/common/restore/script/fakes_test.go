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

package script

import (
	"context"
	"database/sql"
	"errors"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// fakeDB is a core.DB that records executed queries and can inject an exec error.
type fakeDB struct {
	queries []string
	execErr error
}

func (f *fakeDB) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	f.queries = append(f.queries, query)
	return nil, f.execErr
}
func (f *fakeDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}
func (f *fakeDB) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row { return nil }

// fakeRestoreConn is a core.RestoreConn over a core.DB. It carries only ID/DB —
// the session owns the transaction lifecycle.
type fakeRestoreConn struct {
	db core.DB
}

func (c *fakeRestoreConn) ID() int     { return 0 }
func (c *fakeRestoreConn) DB() core.DB { return c.db }

// fakeSession is a core.DatabaseSession that mirrors the per-call session: each
// RunWithEngineResource call commits on fn success and rolls back on fn error. It
// records the lifecycle so tests can assert begin/commit/rollback behaviour.
type fakeSession struct {
	conn       *fakeRestoreConn
	engineErr  error
	committed  bool
	rolledBack bool
}

func (s *fakeSession) Close(_ context.Context) error { return nil }
func (s *fakeSession) RunWithOperationalDB(_ context.Context, _ func(context.Context, core.DB) error) error {
	return core.ErrEngineResourceNotSupported
}
func (s *fakeSession) RunWithEngineResource(ctx context.Context, fn func(context.Context, any) error) error {
	if s.engineErr != nil {
		return s.engineErr
	}
	if err := fn(ctx, s.conn); err != nil {
		s.rolledBack = true
		return err
	}
	s.committed = true
	return nil
}

// newFakeSession builds a per-call-mode session over a fresh fakeDB and returns both.
func newFakeSession() (*fakeSession, *fakeDB) {
	db := &fakeDB{}
	return &fakeSession{conn: &fakeRestoreConn{db: db}}, db
}

// realDBSession is a per-call-mode core.DatabaseSession backed by a real *sql.DB,
// beginning a genuine *sql.Tx for each RunWithEngineResource call and committing
// on success / rolling back on error. Used by the container-backed integration tests.
type realDBSession struct{ db *sql.DB }

func (s realDBSession) Close(_ context.Context) error { return nil }
func (s realDBSession) RunWithOperationalDB(_ context.Context, _ func(context.Context, core.DB) error) error {
	return core.ErrEngineResourceNotSupported
}
func (s realDBSession) RunWithEngineResource(ctx context.Context, fn func(context.Context, any) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	conn := &realRestoreConn{tx: tx}
	if err := fn(ctx, conn); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}

type realRestoreConn struct {
	tx *sql.Tx
}

func (c *realRestoreConn) ID() int     { return 0 }
func (c *realRestoreConn) DB() core.DB { return c.tx }
