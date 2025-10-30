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

package heartbeat

import (
	"context"
	"fmt"
	"time"
)

const (
	heartBeatWriteInterval = 15 * time.Minute

	heartBeatWriteTimeout = 5 * time.Second
)

// heartBeatWriteWorker - interface to write heart beat file
type heartBeatWriter interface {
	Write(ctx context.Context, data Status) error
}

type Worker struct {
	interval            time.Duration
	writer              heartBeatWriter
	terminateWithStatus chan Status
}

func NewWorker(w heartBeatWriter) *Worker {
	return &Worker{
		writer:   w,
		interval: heartBeatWriteInterval,
		// We use a buffered channel to avoid blocking the worker
		// in case of error writing the status.
		terminateWithStatus: make(chan Status, 3),
	}
}

func (w *Worker) SetInterval(v time.Duration) *Worker {
	w.interval = v
	return w
}

func (w *Worker) Terminate(status Status) {
	if w.terminateWithStatus == nil {
		// If the worker is already terminated, we do nothing.
		return
	}
	w.terminateWithStatus <- status
}

// Run - starts the worker to write heartbeats at regular intervals.
func (w *Worker) Run(ctx context.Context) func() error {
	return func() error {
		if err := w.writer.Write(ctx, StatusInProgress); err != nil {
			return fmt.Errorf("write status in-progress: %w", err)
		}
		t := time.NewTicker(w.interval)
		for {
			needExit, err := w.handle(ctx, t)
			if err != nil {
				return fmt.Errorf("handle heartbeat: %w", err)
			}
			if needExit {
				close(w.terminateWithStatus)
				return nil
			}
		}
	}
}

// handle - handles the heartbeat writing logic
// It writes the status in-progress at regular intervals and listens for termination signals.
// If a termination signal is received, it writes the termination status and exits.
// If the context is done, it exits without writing the termination status.
// The bool flag indicates whether the worker should exit.
func (w *Worker) handle(ctx context.Context, t *time.Ticker) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, heartBeatWriteTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return true, nil
	case status := <-w.terminateWithStatus:
		if err := w.writer.Write(ctx, status); err != nil {
			return true, fmt.Errorf("write status termination status %s: %w", status, err)
		}
		return true, nil
	case <-t.C:
		if err := w.writer.Write(ctx, StatusInProgress); err != nil {
			return false, fmt.Errorf("write status in-progress: %w", err)
		}
		return false, nil
	}
}
