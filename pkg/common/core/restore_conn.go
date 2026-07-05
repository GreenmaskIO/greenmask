package core

import (
	"context"
	"fmt"
)

// RestoreConn is the restore-side connection handle returned by
// DatabaseSession.RunWithEngineResource for restore sessions.
//
// The transactional lifecycle is owned by the RestoreSession, not by callers:
// a restorer only needs the active SQL executor and a stable identifier. The
// session begins, commits, and rolls back transactions around the borrowed
// connection (per-call in RestoreSessionDefault, globally in RestoreSessionSingleTx).
type RestoreConn interface {
	// ID returns the stable numeric identifier for this connection in the pool.
	ID() int
	// DB returns the active SQL executor for this connection — the session-managed
	// transaction when one is active, otherwise the underlying connection.
	DB() DB
}

// ExecOnSession runs fn against a restore session connection's active DB. The
// session owns the transaction lifecycle (begin/commit/rollback), so callers
// just execute their statements and return an error to signal failure.
func ExecOnSession(ctx context.Context, session DatabaseSession, fn func(ctx context.Context, db DB) error) error {
	return session.RunWithEngineResource(ctx, func(ctx context.Context, res any) error {
		rc, ok := res.(RestoreConn)
		if !ok {
			return fmt.Errorf("restore session exec: expected RestoreConn, got %T", res)
		}
		return fn(ctx, rc.DB())
	})
}
