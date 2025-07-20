package processor

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	defaultJobCount = 1
)

type taskProducer interface {
	Generate(ctx context.Context, vc *validationcollector.Collector) ([]commonininterfaces.Restorer, error)
}

type schemaRestorer interface {
	RestoreSchema(ctx context.Context) error
}

type DefaultRestoreProcessor struct {
	tp       taskProducer
	taskList []commonininterfaces.Restorer
	st       storages.Storager
	sr       schemaRestorer
	jobs     int
}

func NewDefaultRestoreProcessor(
	tp taskProducer,
	sr schemaRestorer,
) *DefaultRestoreProcessor {
	return &DefaultRestoreProcessor{
		tp:   tp,
		jobs: defaultJobCount,
		sr:   sr,
	}
}

// taskProducer - produces tasks and sends them to tasks channel.
func (p *DefaultRestoreProcessor) taskProducer(
	ctx context.Context,
	tasks chan<- commonininterfaces.Restorer,
) func() error {
	return func() error {
		defer close(tasks)
		for _, t := range p.taskList {
			select {
			case <-ctx.Done():
				return nil
			case tasks <- t:
			}
		}
		return nil
	}
}

// restoreWorker - runs a restoreWorker that consumes tasks from tasks channel and executes them.
func (p *DefaultRestoreProcessor) restoreWorker(
	ctx context.Context,
	tasks <-chan commonininterfaces.Restorer,
	id int,
) error {
	for {
		var task commonininterfaces.Restorer
		var ok bool
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Debug().
				Int("WorkerId", id).
				Msgf("exited due to context cancellation")
			return nil
		case task, ok = <-tasks:
			if !ok {
				log.Ctx(ctx).Debug().
					Err(ctx.Err()).
					Int("WorkerId", id).
					Msgf("exited normally")
				return nil
			}
		}
		log.Ctx(ctx).Debug().
			Int("WorkerId", id).
			Any("ObjectName", task.DebugInfo()).
			Msgf("restoration started")

		if err := task.Restore(ctx); err != nil {
			return fmt.Errorf(`restore task '%s': %w`, task.DebugInfo(), err)
		}

		log.Ctx(ctx).Debug().
			Int("WorkerId", id).
			Any("ObjectName", task.DebugInfo()).
			Msgf("restoration is done")
	}
}

// restoreWorkerRunner - runs restoreWorker.
func (p *DefaultRestoreProcessor) restoreWorkerRunner(
	ctx context.Context, tasks <-chan commonininterfaces.Restorer, jobId int,
) func() error {
	return func() error {
		return p.restoreWorker(ctx, tasks, jobId)
	}
}

// restoreWorkerPlanner - plans retore workers based on the number of jobs and runs them.
//
// It waits until all the workers are done and then closes the done channel to signal the end.
func (p *DefaultRestoreProcessor) restoreWorkerPlanner(
	ctx context.Context,
	tasks <-chan commonininterfaces.Restorer,
	done chan struct{},
) func() error {
	return func() error {
		defer close(done)
		workerEg, gtx := errgroup.WithContext(ctx)
		for j := 0; j < p.jobs; j++ {
			workerEg.Go(
				p.restoreWorkerRunner(gtx, tasks, j),
			)
		}
		if err := workerEg.Wait(); err != nil {
			return err
		}
		return nil
	}
}

func (p *DefaultRestoreProcessor) dataRestore(ctx context.Context) error {
	tasks := make(chan commonininterfaces.Restorer, p.jobs)

	log.Ctx(ctx).Debug().Msgf("planned %d workers", p.jobs)
	done := make(chan struct{})
	eg, egCtx := errgroup.WithContext(ctx)
	// restore worker planner
	eg.Go(p.restoreWorkerPlanner(egCtx, tasks, done))
	// task producer
	eg.Go(p.taskProducer(egCtx, tasks))

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	log.Ctx(ctx).Debug().Msg("all the data have been restored")
	return nil
}

func (p *DefaultRestoreProcessor) Run(ctx context.Context, vc *validationcollector.Collector) error {
	var err error
	p.taskList, err = p.tp.Generate(ctx, vc)
	if err != nil {
		return fmt.Errorf("produce tasks: %w", err)
	}

	if err := p.sr.RestoreSchema(ctx); err != nil {
		return fmt.Errorf("schema restore: %w", err)
	}

	if err := p.dataRestore(ctx); err != nil {
		return fmt.Errorf("data restore: %w", err)
	}
	return nil
}
