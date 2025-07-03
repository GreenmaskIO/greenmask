package heartbeat

import (
	"context"
	"fmt"
	"time"
)

const (
	heartBeatWriteInterval = 15 * time.Minute
)

// heartBeatWriteWorker - interface to write heart beat file
type heartBeatWriter interface {
	Write(ctx context.Context, data Status) error
}

type Worker struct {
	interval time.Duration
	writer   heartBeatWriter
}

func NewWorker(w heartBeatWriter) *Worker {
	return &Worker{
		writer:   w,
		interval: heartBeatWriteInterval,
	}
}

func (w *Worker) SetInterval(v time.Duration) *Worker {
	w.interval = v
	return w
}

func (w *Worker) Run(ctx context.Context, done <-chan struct{}) func() error {
	return func() error {
		if err := w.writer.Write(ctx, StatusInProgress); err != nil {
			return fmt.Errorf("write status in-progress: %w", err)
		}
		t := time.NewTicker(w.interval)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				if err := w.writer.Write(ctx, StatusDone); err != nil {
					return fmt.Errorf("write status done: %w", err)
				}
				return nil
			case <-t.C:
				if err := w.writer.Write(ctx, StatusInProgress); err != nil {
					return fmt.Errorf("write status in-progress: %w", err)
				}
			}
		}
	}
}
