package pipeline

import (
	"context"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// RestoreRuntime holds the live DB session for the duration of a restore
// pipeline run. It is created by RestorePipeline.OpenRuntime and passed
// explicitly only to the Execute phase that requires a live connection.
//
// RestoreRuntime must never be stored inside RestoreRunState — it holds live
// resources and cannot be serialised.
type RestoreRuntime struct {
	Session core.DatabaseSession
}

// Close shuts down the underlying session. Called by withRuntime via a deferred
// cleanup that uses its own context so the close is not skipped on cancellation.
func (r *RestoreRuntime) Close(ctx context.Context) error {
	return r.Session.Close(ctx)
}
