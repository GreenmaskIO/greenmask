package backup

import (
	"context"
	"fmt"

	"github.com/wwoytenko/greenfuscator/internal/runner"
)

type Runner struct {
	snapshot string
	tasks    []runner.Task
}

func NewRunner(snapshot string, tasks []runner.Task) *Runner {
	return &Runner{
		snapshot: snapshot,
		tasks:    tasks,
	}
}

func (r *Runner) Run(ctx context.Context) error {

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	default:

	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, task := range r.tasks {
		if err := task.Execute(ctx); err != nil {
			return err
		}
	}
	return nil
}
