package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type DefaultDumpProcessorV2 struct {
	objectDumpFactory interfaces.ObjectDumpFactoryRegistry
	schemaDumpFactory interfaces.SchemaDumpFactoryRegistry
	jobs              int
	engine            models.DBMSEngine
}

type OptionV2 func(*DefaultDumpProcessorV2) error

func WithJobsV2(jobs int) OptionV2 {
	return func(ddp *DefaultDumpProcessorV2) error {
		if jobs <= 0 {
			return fmt.Errorf("jobs must be positive")
		}
		ddp.jobs = jobs
		return nil
	}
}

func NewDataDumpProcessorV2(
	dumpObjectFactory interfaces.ObjectDumpFactoryRegistry,
	schemaDumpFactory interfaces.SchemaDumpFactoryRegistry,
	engine models.DBMSEngine,
	opts ...OptionV2,
) (*DefaultDumpProcessorV2, error) {
	res := &DefaultDumpProcessorV2{
		objectDumpFactory: dumpObjectFactory,
		schemaDumpFactory: schemaDumpFactory,
		jobs:              defaultJobCount,
		engine:            engine,
	}
	for _, opt := range opts {
		if err := opt(res); err != nil {
			return nil, fmt.Errorf("apply option: %w", err)
		}
	}
	return res, nil
}

func (dr *DefaultDumpProcessorV2) initSchemaDumpers(plan models.DumpPlan) ([]interfaces.SchemaDumper, error) {
	res := make([]interfaces.SchemaDumper, 0, len(plan.SchemaDumpSpecs))
	for _, item := range plan.SchemaDumpSpecs {
		task, err := dr.schemaDumpFactory.New(models.SchemaDumpKind(item.Kind), item)
		if err != nil {
			return nil, fmt.Errorf("new schema dump task: %w", err)
		}
		res = append(res, task)
	}
	return res, nil
}

func (dr *DefaultDumpProcessorV2) initObjectDumpers(plan models.DumpPlan) ([]interfaces.ObjectDumper, error) {
	res := make([]interfaces.ObjectDumper, 0, len(plan.DumpObjectSpecs))
	for _, item := range plan.DumpObjectSpecs {
		task, err := dr.objectDumpFactory.New(item.Kind, item)
		if err != nil {
			return nil, fmt.Errorf("new object dump task: %w", err)
		}
		res = append(res, task)
	}
	return res, nil
}

// Run - runs the dump command.
func (dr *DefaultDumpProcessorV2) Run(ctx context.Context, plan models.DumpPlan) (models.Metadata, error) {
	startedAt := time.Now()

	schemaDumpTasks, err := dr.initSchemaDumpers(plan)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("get schema dump tasks: %w", err)
	}
	schemaDumpStats, err := dr.schemaDump(ctx, schemaDumpTasks)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("schema dump: %w", err)
	}

	dataDumpTasks, err := dr.initObjectDumpers(plan)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("get object dump tasks: %w", err)
	}
	dataDumpStats, err := dr.dataDump(ctx, dataDumpTasks)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("data dump: %w", err)
	}

	return dr.buildMetadata(plan, startedAt, dataDumpStats, schemaDumpStats)
}

func (dr *DefaultDumpProcessorV2) schemaDump(
	ctx context.Context,
	tasks []interfaces.SchemaDumper,
) ([]models.SchemaDumpStat, error) {
	stats := make([]models.SchemaDumpStat, 0, len(tasks))
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		stat, err := task.Dump(ctx)
		if err != nil {
			return nil, fmt.Errorf("dump schema: %w", err)
		}
		stats = append(stats, stat)
	}
	return stats, nil
}

func (dr *DefaultDumpProcessorV2) dataDump(
	ctx context.Context,
	tasks []interfaces.ObjectDumper,
) (map[models.TaskID]models.ObjectDumpStat, error) {
	taskCh := make(chan interfaces.ObjectDumper, dr.jobs)
	statCh := make(chan models.ObjectDumpStat)

	// Collect stats concurrently so workers never block on send.
	// Exits only when statCh is closed by dumpWorkerPlanner — no stats are lost.
	var collected []models.ObjectDumpStat
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		for s := range statCh {
			collected = append(collected, s)
		}
	}()

	log.Ctx(ctx).Debug().Msgf("planned %d workers", dr.jobs)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(dr.taskProducer(egCtx, taskCh, tasks))
	eg.Go(dr.dumpWorkerPlanner(egCtx, taskCh, statCh))

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("at least one worker exited with error: %w", err)
	}
	<-collectDone

	stats := make(map[models.TaskID]models.ObjectDumpStat, len(collected))
	for _, s := range collected {
		stats[s.ID] = s
	}
	log.Ctx(ctx).Debug().Msg("data have been dumped")
	return stats, nil
}

// taskProducer sends tasks to taskCh and closes it when done.
func (dr *DefaultDumpProcessorV2) taskProducer(
	ctx context.Context,
	taskCh chan<- interfaces.ObjectDumper,
	tasks []interfaces.ObjectDumper,
) func() error {
	return func() error {
		defer close(taskCh)
		for _, t := range tasks {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case taskCh <- t:
			}
		}
		return nil
	}
}

// dumpWorkerPlanner spawns all dump workers and closes statCh when they all finish.
func (dr *DefaultDumpProcessorV2) dumpWorkerPlanner(
	ctx context.Context,
	tasks <-chan interfaces.ObjectDumper,
	statCh chan<- models.ObjectDumpStat,
) func() error {
	return func() error {
		defer close(statCh)
		workerEg, gtx := errgroup.WithContext(ctx)
		for j := 0; j < dr.jobs; j++ {
			workerEg.Go(dr.dumpWorkerRunner(gtx, tasks, statCh, j))
		}
		return workerEg.Wait()
	}
}

func (dr *DefaultDumpProcessorV2) dumpWorkerRunner(
	ctx context.Context,
	tasks <-chan interfaces.ObjectDumper,
	statCh chan<- models.ObjectDumpStat,
	jobId int,
) func() error {
	return func() error {
		return dr.dumpWorker(ctx, tasks, statCh, jobId)
	}
}

func (dr *DefaultDumpProcessorV2) dumpWorker(
	ctx context.Context,
	tasks <-chan interfaces.ObjectDumper,
	statCh chan<- models.ObjectDumpStat,
	id int,
) error {
	for {
		var task interfaces.ObjectDumper
		var ok bool
		select {
		case <-ctx.Done():
			log.Ctx(ctx).Debug().Int("WorkerId", id).
				Msg("exited due to context cancellation")
			return ctx.Err()
		case task, ok = <-tasks:
			if !ok {
				log.Ctx(ctx).Debug().Int("WorkerId", id).Msg("exited normally")
				return nil
			}
		}
		log.Ctx(ctx).Debug().
			Int("WorkerId", id).
			Any("ObjectName", task.DebugInfo()).
			Any("ObjectMetadata", task.Meta()).
			Msg("dumping is started")

		stat, err := task.Dump(ctx)
		if err != nil {
			return fmt.Errorf("dump task '%s': %w", task.DebugInfo(), err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case statCh <- stat:
		}

		log.Ctx(ctx).Debug().
			Int("WorkerId", id).
			Any("ObjectName", task.DebugInfo()).
			Any("ObjectMetadata", task.Meta()).
			Msg("dumping is done")
	}
}

func (dr *DefaultDumpProcessorV2) buildSchemaDumpMetadata(stats []models.SchemaDumpStat) *models.SchemaDumpMetadata {
	return models.NewSchemaDumpMetadata(stats)
}

func (dr *DefaultDumpProcessorV2) buildDataDumpMetadata(
	plan models.DumpPlan,
	stats map[models.TaskID]models.ObjectDumpStat,
) *models.DataDumpMetadata {
	taskID2ObjectID := make(map[models.ObjectKind]map[models.TaskID]models.ObjectID)
	objectID2TaskID := make(map[models.ObjectKind]map[models.ObjectID]models.TaskID)
	restorationItems := make(map[models.TaskID]models.RestorationItem)
	for _, s := range stats {
		kindTask2Object, ok := taskID2ObjectID[s.ObjectStat.Kind]
		if !ok {
			kindTask2Object = make(map[models.TaskID]models.ObjectID)
		}
		kindObject2Task, ok := objectID2TaskID[s.ObjectStat.Kind]
		if !ok {
			kindObject2Task = make(map[models.ObjectID]models.TaskID)
		}
		kindTask2Object[s.ID] = s.ObjectStat.ID
		kindObject2Task[s.ObjectStat.ID] = s.ID
		taskID2ObjectID[s.ObjectStat.Kind] = kindTask2Object
		objectID2TaskID[s.ObjectStat.Kind] = kindObject2Task

		restorationItems[s.ID] = models.RestorationItem{
			TaskID:           s.ID,
			ObjectKind:       s.ObjectStat.Kind,
			ObjectID:         s.ObjectStat.ID,
			Engine:           s.Engine,
			ObjectDefinition: s.ObjectDefinition,
			Filename:         s.ObjectStat.Filename,
			RecordCount:      s.RecordCount,
			Compression:      s.ObjectStat.Compression,
		}
	}
	dataDumpStat := models.DataDumpStat{
		RestorationContext: plan.RestorationContext,
		TaskStats:          stats,
		TaskID2ObjectID:    taskID2ObjectID,
		ObjectID2TaskID:    objectID2TaskID,
		RestorationItems:   restorationItems,
	}
	return models.NewDataDumpMetadata(plan.TransformationConfig, dataDumpStat)
}

func (dr *DefaultDumpProcessorV2) buildMetadata(
	plan models.DumpPlan,
	startedAt time.Time,
	dataDumpStats map[models.TaskID]models.ObjectDumpStat,
	schemaDumpStats []models.SchemaDumpStat,
) (models.Metadata, error) {
	meta := models.NewMetadata(
		dr.engine,
		startedAt,
		time.Now(),
		plan.Description,
		plan.Tags,
		plan.IntrospectionResult,
		dr.buildDataDumpMetadata(plan, dataDumpStats),
		dr.buildSchemaDumpMetadata(schemaDumpStats),
		plan.MatchedDatabases,
	)
	return meta, nil
}
