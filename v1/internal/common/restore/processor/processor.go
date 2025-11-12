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

package processor

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

const (
	defaultJobCount = 1
)

type schemaRestorer interface {
	RestoreSchema(ctx context.Context) error
}

type Config struct {
	Jobs           int
	RestoreInOrder bool
}

func (c *Config) SetDefault(ctx context.Context) {
	if c.Jobs <= 0 {
		c.Jobs = defaultJobCount
	}
	if c.RestoreInOrder {
		log.Ctx(ctx).Info().Msg("setting jobs to 1 due to restore-in-order=true")
		// Temporary force single job to ensure order.
		// Later it should be fixed to allow parallelism with order.
		c.Jobs = 1
	}
}

type DefaultRestoreProcessor struct {
	tp  commonininterfaces.RestoreTaskProducer
	st  commonininterfaces.Storager
	sr  schemaRestorer
	cfg Config
}

func NewDefaultRestoreProcessor(
	ctx context.Context,
	tp commonininterfaces.RestoreTaskProducer,
	sr schemaRestorer,
	cfg Config,
) *DefaultRestoreProcessor {
	cfg.SetDefault(ctx)
	return &DefaultRestoreProcessor{
		tp:  tp,
		sr:  sr,
		cfg: cfg,
	}
}

// taskProducer - produces tasks and sends them to tasks channel.
func (p *DefaultRestoreProcessor) taskProducer(
	ctx context.Context,
	tasks chan<- commonininterfaces.Restorer,
) func() error {
	return func() error {
		defer close(tasks)
		for p.tp.Next(ctx) {
			if err := p.tp.Err(); err != nil {
				return err
			}
			task, err := p.tp.Task()
			if err != nil {
				return fmt.Errorf("could not get task: %w", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case tasks <- task:
			}
		}
		return nil
	}
}

func runTask(ctx context.Context, task commonininterfaces.Restorer) error {
	if err := task.Init(ctx); err != nil {
		return fmt.Errorf(`init task: %w`, err)
	}
	defer func() {
		if err := task.Close(ctx); err != nil {
			log.Ctx(ctx).Error().
				Err(err).
				Str(commonmodels.MetaKeyUniqueDumpTaskID, task.DebugInfo()).
				Msg("error closing task")
		}
	}()
	if err := task.Restore(ctx); err != nil {
		return fmt.Errorf(`restore task: %w`, err)
	}
	return nil
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

		if err := runTask(ctx, task); err != nil {
			log.Ctx(ctx).Error().
				Err(err).
				Int("WorkerId", id).
				Any("ObjectName", task.DebugInfo()).
				Msgf("error restoring task")
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
		for j := 0; j < p.cfg.Jobs; j++ {
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
	tasks := make(chan commonininterfaces.Restorer, p.cfg.Jobs)

	log.Ctx(ctx).Debug().Msgf("planned %d workers", p.cfg.Jobs)
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

func (p *DefaultRestoreProcessor) Run(ctx context.Context) error {
	if err := p.sr.RestoreSchema(ctx); err != nil {
		return fmt.Errorf("schema restore: %w", err)
	}

	if err := p.dataRestore(ctx); err != nil {
		return fmt.Errorf("data restore: %w", err)
	}
	return nil
}
