package common

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/storages"
)

const (
	MetadataJsonFileName = "metadata.json"
	HeartBeatFileName    = "heartbeat"
)

const (
	HeartBeatWriteInterval = 15 * time.Minute
)

const (
	HeartBeatDoneContent       = "done"
	HeartBeatInProgressContent = "in-progress"
)

type Connector interface {
	WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
	GetConn() *pgx.Conn
}

type dumpTask interface {
	Dump(ctx context.Context, st storages.Storager) error
	DebugInfo() string
}

// introspector - interface to introspect the database
//
// It embeds the Config builder interface to build the config
type introspector interface {
	Introspect() error
	GetTables() []Table
}

type taskProducer interface {
	Produce(ctx context.Context) ([]dumpTask, error)
	Metadata(ctx context.Context) any
}

// heartBeatWorker - interface to write heart beat file
type heartBeatWorker interface {
	Run(ctx context.Context, done <-chan struct{}) func() error
}

type DumpRuntime struct {
	tp       taskProducer
	st       storages.Storager
	hbw      heartBeatWorker
	jobs     int
	taskList []dumpTask
	dumpID   string
}

func NewDumpRuntime(
	tp taskProducer,
	hbw heartBeatWorker,
	st storages.Storager,
) *DumpRuntime {
	return &DumpRuntime{
		hbw:  hbw,
		tp:   tp,
		st:   st,
		jobs: 1,
	}
}

// SetJobs - sets the number of jobs to run
func (dr *DumpRuntime) SetJobs(v int) *DumpRuntime {
	dr.jobs = v
	return dr
}

// Run - runs the dump command
func (dr *DumpRuntime) Run(ctx context.Context) (err error) {
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
func (dr *DumpRuntime) cwd() {
	dr.dumpID = strconv.FormatInt(time.Now().UnixMilli(), 10)
	dr.st = dr.st.SubStorage(dr.dumpID, true)
}

func (dr *DumpRuntime) schemaOnlyDump(ctx context.Context) error {
	return nil
}

func (dr *DumpRuntime) dataDump(ctx context.Context) error {
	tasks := make(chan dumpTask, dr.jobs)

	log.Debug().Msgf("planned %d workers", dr.jobs)
	done := make(chan struct{})
	eg, gtx := errgroup.WithContext(ctx)
	// write heart beat file writer worker
	eg.Go(dr.hbw.Run(gtx, done))
	// dump worker planner
	eg.Go(dr.dumpWorkerPlanner(gtx, tasks, done))
	// task producer
	eg.Go(dr.taskProducer(gtx, tasks))

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	log.Debug().Msg("all the data have been dumped")
	return nil
}

// taskProducer - produces tasks and sends them to tasks channel.
func (dr *DumpRuntime) taskProducer(ctx context.Context, tasks chan<- dumpTask) func() error {
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

// dumpWorkerPlanner - plans dump workers based on the number of jobs and runs them.
//
// It waits until all the workers are done and then closes the done channel to signal the end.
func (dr *DumpRuntime) dumpWorkerPlanner(ctx context.Context, tasks <-chan dumpTask, done chan struct{}) func() error {
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
func (dr *DumpRuntime) dumpWorkerRunner(
	ctx context.Context, tasks <-chan dumpTask, jobId int,
) func() error {
	return func() error {
		return dr.dumpWorker(ctx, tasks, jobId)
	}
}

// dumpWorker - runs a dumpWorker that consumes tasks from tasks channel and executes them.
func (dr *DumpRuntime) dumpWorker(
	ctx context.Context, tasks <-chan dumpTask, id int,
) error {
	for {
		var task dumpTask
		var ok bool
		select {
		case <-ctx.Done():
			log.Debug().
				Err(ctx.Err()).
				Int("WorkerId", id).
				Msgf("existed due to cancelled context")
			return ctx.Err()
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
			return err
		}

		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}
