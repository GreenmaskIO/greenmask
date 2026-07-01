package pipeline

import (
	"context"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// Runtime holds the live DB session for the duration of a pipeline run.
// It is created by DumpPipeline.OpenRuntime and passed explicitly only to the
// stages that require a live connection (Discover, Execute). Stages that are
// pure/durable (BuildContext, BuildPlan, etc.) do not receive it.
//
// Runtime is never serialised — it must not be stored inside RunState.
type Runtime struct {
	Session core.DatabaseSession
}

// Close shuts down the underlying session. Called by withRuntime via a deferred
// cleanup that uses its own context so the close is not skipped on cancellation.
func (r *Runtime) Close(ctx context.Context) error {
	return r.Session.Close(ctx)
}
