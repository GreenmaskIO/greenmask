package interfaces

import "context"

// RestoreTaskProducer - produces tasks for restoration.
// Works like an iterator.
type RestoreTaskProducer interface {
	// Next - moves to the next task. Returns false if there are no more tasks. Use it in the loop.
	Next(ctx context.Context) bool
	// Err - returns the error if any occurred during task production.
	// Check it after each Next() call.
	Err() error
	// Task - returns the current task. Call it after Next() returns true.
	Task() (Restorer, error)
}
