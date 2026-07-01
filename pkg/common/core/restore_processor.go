package core

import "context"

// RestoreProcessor executes the restore operation.
//
// Implementations coordinate pre-data schema restore, parallel data restore,
// and post-data schema restore using the engine-specific components.
type RestoreProcessor interface {
	Run(ctx context.Context, input RestoreRunInput) error
}
