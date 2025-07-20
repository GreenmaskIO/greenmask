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
	Generate(ctx context.Context, vc *validationcollector.Collector) ([]commonininterfaces.Dumper, error)
	Metadata(ctx context.Context) any
}

// heartBeatWorker - interface to write heart beat file.
type heartBeatWorker interface {
	Run(ctx context.Context, done <-chan struct{}) func() error
}

type schemaDumper interface {
	DumpSchema(ctx context.Context) error
}

type DefaultDumpProcessor struct {
	tp           taskProducer
	st           storages.Storager
	hbw          heartBeatWorker
	jobs         int
	taskList     []commonininterfaces.Dumper
	schemaDumper schemaDumper
	stats        []commonmodels.DumpStat
}

func NewDefaultDumpProcessor(
	tp taskProducer,
	hbw heartBeatWorker,
	schemaDumper schemaDumper,
) *DefaultDumpProcessor {
	return &DefaultDumpProcessor{
		hbw:          hbw,
		tp:           tp,
		jobs:         defaultJobCount,
		schemaDumper: schemaDumper,
	}
}

// SetJobs - sets the number of jobs to run
func (dr *DefaultDumpProcessor) SetJobs(v int) *DefaultDumpProcessor {
	dr.jobs = v
	return dr
}

// Run - runs the dump command
func (dr *DefaultDumpProcessor) Run(ctx context.Context, vc *validationcollector.Collector) (err error) {
	dr.taskList, err = dr.tp.Generate(ctx, vc)
	if err != nil {
		return fmt.Errorf("produce tasks: %w", err)
	}
	if err := dr.schemaDumper.DumpSchema(ctx); err != nil {
		return fmt.Errorf("schema dump: %w", err)
	}

	if err := dr.dataDump(ctx); err != nil {
		return fmt.Errorf("data dump: %w", err)
	}
	return nil
}

func (dr *DefaultDumpProcessor) GetStats() []commonmodels.DumpStat {
	return dr.stats
}

func (dr *DefaultDumpProcessor) dataDump(ctx context.Context) error {
	tasks := make(chan commonininterfaces.Dumper, dr.jobs)

	log.Ctx(ctx).Debug().Msgf("planned %d workers", dr.jobs)
	done := make(chan struct{})
	eg, egCtx := errgroup.WithContext(ctx)
	// write heart beat file writer worker
	eg.Go(dr.hbw.Run(egCtx, done))
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
		dr.stats = append(dr.stats, stat)

		log.Ctx(ctx).Debug().
			Int("WorkerId", id).
			Any("ObjectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}
