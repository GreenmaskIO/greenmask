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
	"database/sql"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/restore/script"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

const (
	defaultJobCount = 1
)

type schemaRestorer interface {
	RestorePreDataSchema(ctx context.Context) error
	RestorePostDataSchema(ctx context.Context) error
}

type Config struct {
	Jobs           int
	RestoreInOrder bool
	DataOnly       bool
	SchemaOnly     bool
	Section        []string
}

// sectionEnabled reports whether the given section should be restored.
// When no explicit sections are configured it falls back to the DataOnly/SchemaOnly flags.
func (p *DefaultRestoreProcessor) sectionEnabled(section commonmodels.DumpSection) bool {
	if len(p.cfg.Section) == 0 {
		switch section {
		case commonmodels.DumpSectionPreData, commonmodels.DumpSectionPostData:
			return !p.cfg.DataOnly
		case commonmodels.DumpSectionData:
			return !p.cfg.SchemaOnly
		}
		return true
	}
	for _, s := range p.cfg.Section {
		if commonmodels.DumpSection(s) == section {
			return true
		}
	}
	return false
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
	tp              interfaces.RestoreTaskProducer
	sr              schemaRestorer
	cfg             Config
	scriptScheduler *script.Scheduler
	connConfig      *mysqlmodels.ConnConfig
}

func NewDefaultRestoreProcessor(
	ctx context.Context,
	tp interfaces.RestoreTaskProducer,
	sr schemaRestorer,
	cfg Config,
	scripts []commonmodels.Script,
	connConfig *mysqlmodels.ConnConfig,
) *DefaultRestoreProcessor {
	cfg.SetDefault(ctx)
	return &DefaultRestoreProcessor{
		tp:              tp,
		sr:              sr,
		cfg:             cfg,
		scriptScheduler: script.NewScheduler(scripts),
		connConfig:      connConfig,
	}
}

// taskProducer - produces tasks and sends them to tasks channel.
func (p *DefaultRestoreProcessor) taskProducer(
	ctx context.Context,
	tasks chan<- interfaces.Restorer,
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

func runTask(ctx context.Context, task interfaces.Restorer) error {
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
	tasks <-chan interfaces.Restorer,
	id int,
) error {
	for {
		var task interfaces.Restorer
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
	ctx context.Context, tasks <-chan interfaces.Restorer, jobId int,
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
	tasks <-chan interfaces.Restorer,
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
	tasks := make(chan interfaces.Restorer, p.cfg.Jobs)

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

// buildTxExec opens a MySQL connection from connConfig and returns a TxExec
// that executes SQL statements against it, plus a cleanup that closes the connection.
// Returns (nil, no-op, nil) when connConfig is not set (no SQL scripts possible).
func (p *DefaultRestoreProcessor) buildTxExec(ctx context.Context) (script.TxExec, func(), error) {
	if p.connConfig == nil {
		return nil, func() {}, nil
	}
	uri, err := p.connConfig.URI()
	if err != nil {
		return nil, func() {}, fmt.Errorf("build script TxExec: get connection URI: %w", err)
	}
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, func() {}, fmt.Errorf("build script TxExec: open connection: %w", err)
	}
	exec := script.TxExec(func(ctx context.Context, query string) error {
		_, err := db.ExecContext(ctx, query)
		return err
	})
	return exec, func() {
		if err := db.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close script db connection")
		}
	}, nil
}

func (p *DefaultRestoreProcessor) restorePreDataSchema(ctx context.Context) error {
	exec, closeDB, err := p.buildTxExec(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	if err := p.scriptScheduler.Exec(
		ctx, exec, commonmodels.DumpSectionPreData, commonmodels.ScriptEventTypeBefore,
	); err != nil {
		return fmt.Errorf("execute scripts section='%s' when='%s': %w", commonmodels.DumpSectionPreData, commonmodels.ScriptEventTypeBefore, err)
	}
	if err := p.sr.RestorePreDataSchema(ctx); err != nil {
		return fmt.Errorf("pre-data schema restore: %w", err)
	}
	if err := p.scriptScheduler.Exec(
		ctx, exec, commonmodels.DumpSectionPreData, commonmodels.ScriptEventTypeAfter,
	); err != nil {
		return fmt.Errorf("execute scripts section='%s' when='%s': %w", commonmodels.DumpSectionPreData, commonmodels.ScriptEventTypeAfter, err)
	}
	return nil
}

func (p *DefaultRestoreProcessor) restoreData(ctx context.Context) error {
	exec, closeDB, err := p.buildTxExec(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	if err := p.scriptScheduler.Exec(
		ctx, exec, commonmodels.DumpSectionData, commonmodels.ScriptEventTypeBefore,
	); err != nil {
		return fmt.Errorf("execute scripts section='%s' when='%s': %w", commonmodels.DumpSectionData, commonmodels.ScriptEventTypeBefore, err)
	}
	if err := p.dataRestore(ctx); err != nil {
		return fmt.Errorf("data restore: %w", err)
	}
	if err := p.scriptScheduler.Exec(
		ctx, exec, commonmodels.DumpSectionData, commonmodels.ScriptEventTypeAfter,
	); err != nil {
		return fmt.Errorf("execute scripts section='%s' when='%s': %w", commonmodels.DumpSectionData, commonmodels.ScriptEventTypeAfter, err)
	}
	return nil
}

func (p *DefaultRestoreProcessor) restorePostDataSchema(ctx context.Context) error {
	exec, closeDB, err := p.buildTxExec(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	if err := p.scriptScheduler.Exec(
		ctx, exec, commonmodels.DumpSectionPostData, commonmodels.ScriptEventTypeBefore,
	); err != nil {
		return fmt.Errorf("execute scripts section='%s' when='%s': %w", commonmodels.DumpSectionPostData, commonmodels.ScriptEventTypeBefore, err)
	}
	if err := p.sr.RestorePostDataSchema(ctx); err != nil {
		return fmt.Errorf("post-data schema restore: %w", err)
	}
	if err := p.scriptScheduler.Exec(
		ctx, exec, commonmodels.DumpSectionPostData, commonmodels.ScriptEventTypeAfter,
	); err != nil {
		return fmt.Errorf("execute scripts section='%s' when='%s': %w", commonmodels.DumpSectionPostData, commonmodels.ScriptEventTypeAfter, err)
	}
	return nil
}

func (p *DefaultRestoreProcessor) Run(ctx context.Context) error {
	if p.sectionEnabled(commonmodels.DumpSectionPreData) {
		if err := p.restorePreDataSchema(ctx); err != nil {
			return fmt.Errorf("pre-data schema restore: %w", err)
		}
	}

	if p.sectionEnabled(commonmodels.DumpSectionData) {
		if err := p.restoreData(ctx); err != nil {
			return fmt.Errorf("data restore: %w", err)
		}
	}

	if p.sectionEnabled(commonmodels.DumpSectionPostData) {
		if err := p.restorePostDataSchema(ctx); err != nil {
			return fmt.Errorf("post-data schema restore: %w", err)
		}
	}
	return nil
}
