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
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/restore/script"
	"github.com/greenmaskio/greenmask/pkg/common/restore/taskmapper"
)

const defaultJobCount = 1

// defaultSessionFinalizeTimeout bounds the commit/rollback performed by
// DoneWithError so a cancelled run context still gets a chance to finalize.
const defaultSessionFinalizeTimeout = 30 * time.Second

var _ core.RestoreProcessor = (*DefaultRestoreProcessorV2)(nil)

type DefaultRestoreProcessorV2 struct {
	objectFactory core.ObjectRestoreFactoryRegistry
	schemaFactory core.SchemaRestoreFactoryRegistry
	engine        core.DBMSEngine
	jobs          int
}

type RestoreOptionV2 func(*DefaultRestoreProcessorV2) error

func WithRestoreJobsV2(jobs int) RestoreOptionV2 {
	return func(p *DefaultRestoreProcessorV2) error {
		if jobs <= 0 {
			return fmt.Errorf("jobs must be positive")
		}
		p.jobs = jobs
		return nil
	}
}

func NewDefaultRestoreProcessorV2(
	objectFactory core.ObjectRestoreFactoryRegistry,
	schemaFactory core.SchemaRestoreFactoryRegistry,
	engine core.DBMSEngine,
	opts ...RestoreOptionV2,
) (*DefaultRestoreProcessorV2, error) {
	p := &DefaultRestoreProcessorV2{
		objectFactory: objectFactory,
		schemaFactory: schemaFactory,
		engine:        engine,
		jobs:          defaultJobCount,
	}
	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}
	return p, nil
}

func (p *DefaultRestoreProcessorV2) Run(ctx context.Context, input core.RestoreRunInput) (err error) {
	if err = input.Validate(); err != nil {
		return fmt.Errorf("validate restore run input: %w", err)
	}

	sess, ok := input.Session.(core.RestoreSession)
	if !ok {
		return fmt.Errorf("restore processor: session does not implement core.RestoreSession (%T)", input.Session)
	}
	if err = sess.Init(ctx); err != nil {
		return fmt.Errorf("init restore session: %w", err)
	}
	defer func() {
		// Detached, time-bounded context so a cancelled ctx still allows rollback.
		doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultSessionFinalizeTimeout)
		defer cancel()
		if doneErr := sess.DoneWithError(doneCtx, err); doneErr != nil {
			err = errors.Join(err, fmt.Errorf("finalize restore session: %w", doneErr))
		}
	}()

	if input.Instruction.Jobs > 0 {
		p.jobs = input.Instruction.Jobs
	}

	var preDataSpecs, postDataSpecs []core.SchemaRestoreSpec
	for _, s := range input.Plan.SchemaRestoreSpecs {
		switch s.Section {
		case core.DumpSectionPreData:
			preDataSpecs = append(preDataSpecs, s)
		case core.DumpSectionPostData:
			postDataSpecs = append(postDataSpecs, s)
		}
	}

	scriptScheduler := script.NewScheduler(input.Instruction.Scripts)

	if p.sectionEnabled(input.Instruction, core.DumpSectionPreData) {
		if err := p.restoreSchemaSection(ctx, input, preDataSpecs, scriptScheduler, core.DumpSectionPreData); err != nil {
			return fmt.Errorf("pre-data schema restore: %w", err)
		}
	}

	if p.sectionEnabled(input.Instruction, core.DumpSectionData) {
		if err := p.restoreData(ctx, input, scriptScheduler); err != nil {
			return fmt.Errorf("data restore: %w", err)
		}
	}

	if p.sectionEnabled(input.Instruction, core.DumpSectionPostData) {
		if err := p.restoreSchemaSection(ctx, input, postDataSpecs, scriptScheduler, core.DumpSectionPostData); err != nil {
			return fmt.Errorf("post-data schema restore: %w", err)
		}
	}

	return nil
}

func (p *DefaultRestoreProcessorV2) sectionEnabled(instr core.RestoreInstruction, section core.DumpSection) bool {
	if len(instr.Section) > 0 {
		for _, s := range instr.Section {
			if core.DumpSection(s) == section {
				return true
			}
		}
		return false
	}
	switch section {
	case core.DumpSectionPreData, core.DumpSectionPostData:
		return !instr.DataOnly
	case core.DumpSectionData:
		return !instr.SchemaOnly
	}
	return true
}

func (p *DefaultRestoreProcessorV2) restoreSchemaSection(
	ctx context.Context,
	input core.RestoreRunInput,
	specs []core.SchemaRestoreSpec,
	sched *script.Scheduler,
	section core.DumpSection,
) error {
	if err := sched.Exec(ctx, input.Session, section, core.ScriptEventTypeBefore); err != nil {
		return fmt.Errorf("scripts before section=%s: %w", section, err)
	}

	for _, spec := range specs {
		sr, err := p.schemaFactory.New(spec.Kind, spec)
		if err != nil {
			return fmt.Errorf("build schema restorer kind=%s: %w", spec.Kind, err)
		}
		log.Ctx(ctx).Debug().Str("restorer", sr.DebugInfo()).Str("section", string(section)).Msg("restoring schema")
		if err := sr.Restore(ctx, input.Session, input.Conn, input.St); err != nil {
			return fmt.Errorf("restore schema %s: %w", sr.DebugInfo(), err)
		}
	}

	if err := sched.Exec(ctx, input.Session, section, core.ScriptEventTypeAfter); err != nil {
		return fmt.Errorf("scripts after section=%s: %w", section, err)
	}
	return nil
}

func (p *DefaultRestoreProcessorV2) restoreData(
	ctx context.Context,
	input core.RestoreRunInput,
	sched *script.Scheduler,
) error {
	if err := sched.Exec(ctx, input.Session, core.DumpSectionData, core.ScriptEventTypeBefore); err != nil {
		return fmt.Errorf("scripts before data: %w", err)
	}

	if err := p.runDataRestorers(ctx, input); err != nil {
		return err
	}

	if err := sched.Exec(ctx, input.Session, core.DumpSectionData, core.ScriptEventTypeAfter); err != nil {
		return fmt.Errorf("scripts after data: %w", err)
	}
	return nil
}

func (p *DefaultRestoreProcessorV2) runDataRestorers(ctx context.Context, input core.RestoreRunInput) error {
	specs := input.Plan.ObjectRestoreSpecs
	mapper := taskmapper.NewTaskResolver()

	var producer specProducer
	if input.Instruction.RestoreInOrder && input.Plan.RestorationContext.HasTopologicalOrder {
		log.Ctx(ctx).Info().Msg("restoring tables in topological order")
		producer = &orderedProducer{
			specs:  specs,
			order:  input.Plan.RestorationContext.RestorationOrder,
			deps:   input.Plan.RestorationContext.TaskDependencies,
			mapper: mapper,
		}
	} else {
		if input.Instruction.RestoreInOrder {
			log.Ctx(ctx).Warn().Msg("restore-in-order requested but no topological order in dump; falling back to unordered restore")
		}
		producer = &unorderedProducer{specs: specs}
	}

	return p.runWithProducer(ctx, input, producer, mapper)
}

func (p *DefaultRestoreProcessorV2) runWithProducer(
	ctx context.Context,
	input core.RestoreRunInput,
	producer specProducer,
	mapper core.TaskMapper,
) error {
	taskCh := make(chan core.ObjectRestoreSpec, p.jobs)
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return producer.Produce(egCtx, taskCh)
	})

	for i := 0; i < p.jobs; i++ {
		workerID := i
		eg.Go(func() error {
			for spec := range taskCh {
				// Bind worker/task_id to a per-task logger stored in the context so
				// every downstream log line (object restore, reader/writer) carries
				// them without having to thread the fields through each call.
				taskLogger := log.Ctx(egCtx).With().
					Int("worker", workerID).
					Int("task_id", int(spec.TaskID)).
					Logger()
				taskCtx := taskLogger.WithContext(egCtx)
				taskLogger.Debug().Str("kind", string(spec.Kind)).Msg("dispatching restore task")
				if err := p.restoreOneObject(taskCtx, input, spec); err != nil {
					return err
				}
				mapper.SetTaskCompleted(spec.TaskID)
			}
			return nil
		})
	}

	return eg.Wait()
}

func (p *DefaultRestoreProcessorV2) restoreOneObject(
	ctx context.Context,
	input core.RestoreRunInput,
	spec core.ObjectRestoreSpec,
) error {
	restorer, err := p.objectFactory.New(spec.Kind, spec)
	if err != nil {
		return fmt.Errorf("build object restorer kind=%s: %w", spec.Kind, err)
	}
	log.Ctx(ctx).Debug().Str("kind", string(spec.Kind)).Fields(restorer.Meta()).Msg("restoring object")
	if err := restorer.Restore(ctx, input.Session, input.Conn, input.St); err != nil {
		return fmt.Errorf("restore object %s: %w", restorer.DebugInfo(), err)
	}
	return nil
}
