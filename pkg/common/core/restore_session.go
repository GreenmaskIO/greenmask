package core

import "context"

// RestoreSession is the restore-specific session contract. It extends
// DatabaseSession with an explicit transactional lifecycle so the restore
// processor — not individual restorers — owns commit/rollback.
//
// The processor calls Init at the start of Run and DoneWithError(runErr) at the
// end (before Close). Restorers and scripts inherit the transactional behaviour
// automatically through the one shared session, via RunWithEngineResource /
// ExecOnSession.
type RestoreSession interface {
	DatabaseSession
	// Init opens connections and, for transactional sessions, begins the
	// transactions handed out by RunWithEngineResource.
	Init(ctx context.Context) error
	// DoneWithError finalizes the session: commit when cause is nil, rollback
	// when cause is non-nil. Called once, before Close.
	DoneWithError(ctx context.Context, cause error) error
}
