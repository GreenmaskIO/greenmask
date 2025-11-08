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
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	defaultJobCount = 1
)

type taskProducer interface {
	Produce(ctx context.Context, vc *validationcollector.Collector) (
		[]commonininterfaces.Dumper,
		commonmodels.RestorationContext,
		error,
	)
}

type schemaDumper interface {
	DumpSchema(ctx context.Context) error
}

type DefaultDumpProcessor struct {
	tp           taskProducer
	st           storages.Storager
	jobs         int
	taskList     []commonininterfaces.Dumper
	schemaDumper schemaDumper
	taskStats    map[commonmodels.TaskID]commonmodels.TaskStat
}

func NewDefaultDumpProcessor(
	tp taskProducer,
	schemaDumper schemaDumper,
) *DefaultDumpProcessor {
	return &DefaultDumpProcessor{
		tp:           tp,
		jobs:         defaultJobCount,
		schemaDumper: schemaDumper,
		taskStats:    make(map[commonmodels.TaskID]commonmodels.TaskStat),
	}
}

// SetJobs - sets the number of jobs to run
func (dr *DefaultDumpProcessor) SetJobs(v int) *DefaultDumpProcessor {
	dr.jobs = v
	return dr
}

// Run - runs the dump command
func (dr *DefaultDumpProcessor) Run(
	ctx context.Context,
	vc *validationcollector.Collector,
) (commonmodels.DumpStat, error) {
	var err error
	var restorationContext commonmodels.RestorationContext
	dr.taskList, restorationContext, err = dr.tp.Produce(ctx, vc)
	if err != nil {
		return commonmodels.DumpStat{}, fmt.Errorf("produce tasks: %w", err)
	}
	if err := dr.schemaDumper.DumpSchema(ctx); err != nil {
		return commonmodels.DumpStat{}, fmt.Errorf("schema dump: %w", err)
	}

	if err := dr.dataDump(ctx); err != nil {
		return commonmodels.DumpStat{}, fmt.Errorf("data dump: %w", err)
	}
	taskID2ObjectID := make(map[commonmodels.ObjectKind]map[commonmodels.TaskID]commonmodels.ObjectID)
	objectID2TaskID := make(map[commonmodels.ObjectKind]map[commonmodels.ObjectID]commonmodels.TaskID)
	restorationItems := make(map[commonmodels.TaskID]commonmodels.RestorationItem, len(dr.taskList))
	for _, s := range dr.taskStats {
		kindTask2Object, ok := taskID2ObjectID[s.ObjectStat.Kind]
		if !ok {
			kindTask2Object = make(map[commonmodels.TaskID]commonmodels.ObjectID)
		}
		kindObject2Task, ok := objectID2TaskID[s.ObjectStat.Kind]
		if !ok {
			kindObject2Task = make(map[commonmodels.ObjectID]commonmodels.TaskID)
		}
		kindTask2Object[s.ID] = s.ObjectStat.ID
		kindObject2Task[s.ObjectStat.ID] = s.ID
		taskID2ObjectID[s.ObjectStat.Kind] = kindTask2Object
		objectID2TaskID[s.ObjectStat.Kind] = kindObject2Task

		restorationItems[s.ID] = commonmodels.RestorationItem{
			TaskID:           s.ID,
			ObjectKind:       s.ObjectStat.Kind,
			ObjectID:         s.ObjectStat.ID,
			Engine:           s.Engine,
			ObjectDefinition: s.ObjectDefinition,
			Filename:         s.ObjectStat.Filename,
			RecordCount:      s.RecordCount,
		}
	}
	return commonmodels.DumpStat{
		RestorationContext: restorationContext,
		TaskStats:          dr.taskStats,
		TaskID2ObjectID:    taskID2ObjectID,
		ObjectID2TaskID:    objectID2TaskID,
		RestorationItems:   restorationItems,
	}, nil
}

func (dr *DefaultDumpProcessor) dataDump(ctx context.Context) error {
	tasks := make(chan commonininterfaces.Dumper, dr.jobs)

	log.Ctx(ctx).Debug().Msgf("planned %d workers", dr.jobs)
	done := make(chan struct{})
	eg, egCtx := errgroup.WithContext(ctx)
	// dump worker planner
	eg.Go(dr.dumpWorkerPlanner(egCtx, tasks, done))
	// task producer
	eg.Go(dr.taskProducer(egCtx, tasks))

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	log.Ctx(ctx).Debug().Msg("all the data have been dumped")
	return nil
}

// taskProducer - produces tasks and sends them to tasks channel.
func (dr *DefaultDumpProcessor) taskProducer(ctx context.Context, tasks chan<- commonininterfaces.Dumper) func() error {
	return func() error {
		defer close(tasks)
		for _, t := range dr.taskList {
			select {
			case <-ctx.Done():
				return nil
			case tasks <- t:
			}
		}
		return nil
	}
}

// dumpWorkerPlanner - plans dump workers based on the number of jobs and runs them.
//
// It waits until all the workers are done and then closes the done channel to signal the end.
func (dr *DefaultDumpProcessor) dumpWorkerPlanner(
	ctx context.Context,
	tasks <-chan commonininterfaces.Dumper,
	done chan struct{},
) func() error {
	return func() error {
		defer close(done)
		workerEg, gtx := errgroup.WithContext(ctx)
		for j := 0; j < dr.jobs; j++ {
			workerEg.Go(
				dr.dumpWorkerRunner(gtx, tasks, j),
			)
		}
		if err := workerEg.Wait(); err != nil {
			return err
		}
		return nil
	}
}

// dumpWorkerRunner - runs dumpWorker or validateDumpWorker depending on the mode.
func (dr *DefaultDumpProcessor) dumpWorkerRunner(
	ctx context.Context, tasks <-chan commonininterfaces.Dumper, jobId int,
) func() error {
	return func() error {
		return dr.dumpWorker(ctx, tasks, jobId)
	}
}

// dumpWorker - runs a dumpWorker that consumes tasks from tasks channel and executes them.
func (dr *DefaultDumpProcessor) dumpWorker(
	ctx context.Context,
	tasks <-chan commonininterfaces.Dumper,
	id int,
) error {
	for {
		var task commonininterfaces.Dumper
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
			Msgf("dumping started")

		stat, err := task.Dump(ctx)
		if err != nil {
			return fmt.Errorf(`dump task '%s': %w`, task.DebugInfo(), err)
		}
		dr.taskStats[stat.ID] = stat

		log.Ctx(ctx).Debug().
			Int("WorkerId", id).
			Any("ObjectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}
