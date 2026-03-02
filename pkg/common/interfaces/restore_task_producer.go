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
