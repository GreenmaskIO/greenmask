package datadump

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	defaultRuntimeJobs = 1
)

type dumpTask interface {
	Dump(ctx context.Context, st storages.Storager) error
	DebugInfo() string
}

type taskProducer interface {
	Produce(ctx context.Context) ([]dumpTask, error)
	Metadata(ctx context.Context) any
}

// heartBeatWorker - interface to write heart beat file.
type heartBeatWorker interface {
	Run(ctx context.Context, done <-chan struct{}) func() error
}

type DefaultDataDumper struct {
	tp       taskProducer
	st       storages.Storager
	hbw      heartBeatWorker
	jobs     int
	taskList []dumpTask
	dumpID   string
}

func NewDefaultDataDumper(
	tp taskProducer,
	hbw heartBeatWorker,
	st storages.Storager,
) *DefaultDataDumper {
	return &DefaultDataDumper{
		hbw:  hbw,
		tp:   tp,
		st:   st,
		jobs: defaultRuntimeJobs,
	}
}

// SetJobs - sets the number of jobs to run
func (dr *DefaultDataDumper) SetJobs(v int) *DefaultDataDumper {
	dr.jobs = v
	return dr
}

// Run - runs the dump command
func (dr *DefaultDataDumper) Run(ctx context.Context) (err error) {
	dr.cwd()
	dr.taskList, err = dr.tp.Produce(ctx)
	if err != nil {
		return fmt.Errorf("produce tasks: %w", err)
	}
	if err := dr.schemaOnlyDump(ctx); err != nil {
		return fmt.Errorf("schema only dump: %w", err)
	}

	if err := dr.dataDump(ctx); err != nil {
		return fmt.Errorf("data dump: %w", err)
	}
	return nil
}

// cwd - change working directory
//
// It creates the directory with dumpID as the name
func (dr *DefaultDataDumper) cwd() {
	dr.dumpID = strconv.FormatInt(time.Now().UnixMilli(), 10)
	dr.st = dr.st.SubStorage(dr.dumpID, true)
}

func (dr *DefaultDataDumper) schemaOnlyDump(ctx context.Context) error {
	return nil
}

func (dr *DefaultDataDumper) dataDump(ctx context.Context) error {
	tasks := make(chan dumpTask, dr.jobs)

	log.Debug().Msgf("planned %d workers", dr.jobs)
	done := make(chan struct{})
	eg, gctx := errgroup.WithContext(ctx)
	// write heart beat file writer worker
	eg.Go(dr.hbw.Run(gctx, done))
	// dump worker planner
	eg.Go(dr.dumpWorkerPlanner(gctx, tasks, done))
	// task producer
	eg.Go(dr.taskProducer(gctx, tasks))

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	log.Debug().Msg("all the data have been dumped")
	return nil
}

// taskProducer - produces tasks and sends them to tasks channel.
func (dr *DefaultDataDumper) taskProducer(ctx context.Context, tasks chan<- dumpTask) func() error {
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
func (dr *DefaultDataDumper) dumpWorkerPlanner(ctx context.Context, tasks <-chan dumpTask, done chan struct{}) func() error {
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
func (dr *DefaultDataDumper) dumpWorkerRunner(
	ctx context.Context, tasks <-chan dumpTask, jobId int,
) func() error {
	return func() error {
		return dr.dumpWorker(ctx, tasks, jobId)
	}
}

// dumpWorker - runs a dumpWorker that consumes tasks from tasks channel and executes them.
func (dr *DefaultDataDumper) dumpWorker(
	ctx context.Context, tasks <-chan dumpTask, id int,
) error {
	for {
		var task dumpTask
		var ok bool
		select {
		case <-ctx.Done():
			log.Debug().
				Int("WorkerId", id).
				Msgf("exited due to context cancellation")
			return nil
		case task, ok = <-tasks:
			if !ok {
				log.Debug().
					Err(ctx.Err()).
					Int("WorkerId", id).
					Msgf("exited normally")
				return nil
			}
		}
		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping started")

		if err := task.Dump(ctx, dr.st); err != nil {
			return fmt.Errorf(`dump task '%s': %w`, task.DebugInfo(), err)
		}

		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}
