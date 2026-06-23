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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// --- Unit tests (no container) -------------------------------------------------

// TestNewRestoreSession_poolSize verifies both constructors record the pool size.
func TestNewRestoreSession_poolSize(t *testing.T) {
	assert.Equal(t, 4, NewRestoreSessionDefault(nil, 4).poolSize)
	assert.Equal(t, 7, NewRestoreSessionSingleTx(nil, 7).poolSize)
}

// TestRestoreSession_implementsRestoreSession is a compile-time-style guard that
// both session types satisfy core.RestoreSession.
func TestRestoreSession_implementsRestoreSession(t *testing.T) {
	var _ core.RestoreSession = NewRestoreSessionDefault(nil, 1)
	var _ core.RestoreSession = NewRestoreSessionSingleTx(nil, 1)
}

// TestRestoreSession_RunWithOperationalDB_unsupported verifies the operational DB
// path is intentionally disabled for restore — all writes go via RestoreConn.
func TestRestoreSession_RunWithOperationalDB_unsupported(t *testing.T) {
	tests := []struct {
		name string
		sess core.RestoreSession
	}{
		{"default", NewRestoreSessionDefault(nil, 1)},
		{"single-tx", NewRestoreSessionSingleTx(nil, 1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.sess.RunWithOperationalDB(context.Background(), func(context.Context, core.DB) error {
				t.Fatal("fn must not be called")
				return nil
			})
			assert.ErrorIs(t, err, core.ErrEngineResourceNotSupported)
		})
	}
}

// TestRestoreSession_RunWithEngineResource_uninitialised verifies borrowing before
// Init fails instead of panicking on a nil queue.
func TestRestoreSession_RunWithEngineResource_uninitialised(t *testing.T) {
	tests := []struct {
		name string
		sess core.RestoreSession
	}{
		{"default", NewRestoreSessionDefault(nil, 1)},
		{"single-tx", NewRestoreSessionSingleTx(nil, 1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.sess.RunWithEngineResource(context.Background(), func(context.Context, any) error {
				t.Fatal("fn must not be called")
				return nil
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not initialised")
		})
	}
}

// TestRestoreWorkerConn_DB_prefersTx verifies DB() returns the active transaction
// when one is set, otherwise the underlying connection.
func TestRestoreWorkerConn_DB_prefersTx(t *testing.T) {
	tx := &sql.Tx{}
	rc := &restoreWorkerConn{id: 3, tx: tx}
	assert.Equal(t, 3, rc.ID())
	gotTx, ok := rc.DB().(*sql.Tx)
	require.True(t, ok, "DB() should return the active *sql.Tx")
	assert.Same(t, tx, gotTx)
}

// TestRestoreWorkerConn_txHelpers_noop verifies commit/rollback are no-ops when no
// transaction is open.
func TestRestoreWorkerConn_txHelpers_noop(t *testing.T) {
	rc := &restoreWorkerConn{id: 1}
	assert.NoError(t, rc.commit())
	assert.NoError(t, rc.rollback())
}

// TestRestoreSessionDefault_DoneWithError_noop verifies DoneWithError never errors
// for the default session (no global transaction to finalize).
func TestRestoreSessionDefault_DoneWithError_noop(t *testing.T) {
	s := NewRestoreSessionDefault(nil, 1)
	assert.NoError(t, s.DoneWithError(context.Background(), nil))
	assert.NoError(t, s.DoneWithError(context.Background(), errors.New("boom")))
}

// --- Integration tests (shared container) --------------------------------------

func TestRestoreSession_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed restore session test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	mysqlContainer, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("testdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("testpass"),
	)
	require.NoError(t, err)
	defer func() { _ = mysqlContainer.Terminate(ctx) }()

	uri, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	connCfg, err := connConfigFromURI(uri)
	require.NoError(t, err)

	verifyDB, err := sql.Open("mysql", uri)
	require.NoError(t, err)
	defer func() { _ = verifyDB.Close() }()

	countRows := func(t *testing.T, table string) int {
		t.Helper()
		var count int
		require.NoError(t, verifyDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count))
		return count
	}

	t.Run("default: per-call commits on success", func(t *testing.T) {
		_, err := verifyDB.ExecContext(ctx, "CREATE TABLE def_commit (id INT PRIMARY KEY)")
		require.NoError(t, err)

		s := NewRestoreSessionDefault(connCfg, 2)
		require.NoError(t, s.Init(ctx))

		err = s.RunWithEngineResource(ctx, func(ctx context.Context, res any) error {
			rc := res.(core.RestoreConn)
			_, err := rc.DB().ExecContext(ctx, "INSERT INTO def_commit (id) VALUES (1)")
			return err
		})
		require.NoError(t, err)
		require.NoError(t, s.DoneWithError(ctx, nil))
		require.NoError(t, s.Close(ctx))

		assert.Equal(t, 1, countRows(t, "def_commit"))
	})

	t.Run("default: per-call rolls back when fn errors", func(t *testing.T) {
		_, err := verifyDB.ExecContext(ctx, "CREATE TABLE def_rb (id INT PRIMARY KEY)")
		require.NoError(t, err)

		s := NewRestoreSessionDefault(connCfg, 1)
		require.NoError(t, s.Init(ctx))

		boom := errors.New("boom")
		err = s.RunWithEngineResource(ctx, func(ctx context.Context, res any) error {
			rc := res.(core.RestoreConn)
			_, err := rc.DB().ExecContext(ctx, "INSERT INTO def_rb (id) VALUES (1)")
			require.NoError(t, err)
			return boom
		})
		require.ErrorIs(t, err, boom)
		require.NoError(t, s.Close(ctx))

		assert.Equal(t, 0, countRows(t, "def_rb"))
	})

	t.Run("single-tx: DoneWithError(nil) commits everything", func(t *testing.T) {
		_, err := verifyDB.ExecContext(ctx, "CREATE TABLE stx_commit (id INT PRIMARY KEY)")
		require.NoError(t, err)

		s := NewRestoreSessionSingleTx(connCfg, 1)
		require.NoError(t, s.Init(ctx))

		err = s.RunWithEngineResource(ctx, func(ctx context.Context, res any) error {
			rc := res.(core.RestoreConn)
			_, err := rc.DB().ExecContext(ctx, "INSERT INTO stx_commit (id) VALUES (1)")
			return err
		})
		require.NoError(t, err)

		// Still inside the shared tx — a separate session must not see the row yet.
		assert.Equal(t, 0, countRows(t, "stx_commit"))

		require.NoError(t, s.DoneWithError(ctx, nil))
		require.NoError(t, s.Close(ctx))

		assert.Equal(t, 1, countRows(t, "stx_commit"))
	})

	t.Run("single-tx: DoneWithError(err) rolls back everything", func(t *testing.T) {
		_, err := verifyDB.ExecContext(ctx, "CREATE TABLE stx_rb (id INT PRIMARY KEY)")
		require.NoError(t, err)

		s := NewRestoreSessionSingleTx(connCfg, 1)
		require.NoError(t, s.Init(ctx))

		err = s.RunWithEngineResource(ctx, func(ctx context.Context, res any) error {
			rc := res.(core.RestoreConn)
			_, err := rc.DB().ExecContext(ctx, "INSERT INTO stx_rb (id) VALUES (1)")
			return err
		})
		require.NoError(t, err)

		require.NoError(t, s.DoneWithError(ctx, errors.New("run failed")))
		require.NoError(t, s.Close(ctx))

		assert.Equal(t, 0, countRows(t, "stx_rb"))
	})

	t.Run("connection is returned to the pool after use", func(t *testing.T) {
		s := NewRestoreSessionDefault(connCfg, 1)
		require.NoError(t, s.Init(ctx))
		defer func() { _ = s.Close(ctx) }()

		var firstID int
		require.NoError(t, s.RunWithEngineResource(ctx, func(_ context.Context, res any) error {
			firstID = res.(core.RestoreConn).ID()
			return nil
		}))
		// A second borrow on a single-connection pool only succeeds if the first
		// borrow returned its connection.
		require.NoError(t, s.RunWithEngineResource(ctx, func(_ context.Context, res any) error {
			assert.Equal(t, firstID, res.(core.RestoreConn).ID())
			return nil
		}))
	})

	t.Run("borrow honours context cancellation when pool is drained", func(t *testing.T) {
		s := NewRestoreSessionDefault(connCfg, 1)
		require.NoError(t, s.Init(ctx))
		defer func() { _ = s.Close(ctx) }()

		// Hold the only connection so the pool is empty.
		held := make(chan struct{})
		released := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			_ = s.RunWithEngineResource(ctx, func(_ context.Context, _ any) error {
				close(released)
				<-held
				return nil
			})
		})
		<-released

		cctx, ccancel := context.WithCancel(ctx)
		ccancel()
		err := s.RunWithEngineResource(cctx, func(context.Context, any) error {
			t.Fatal("fn must not run when context is cancelled")
			return nil
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "acquire restore conn")

		close(held)
		wg.Wait()
	})

	t.Run("close on uninitialised session is safe", func(t *testing.T) {
		s := NewRestoreSessionDefault(connCfg, 1)
		assert.NoError(t, s.Close(ctx))
	})
}
