package taskproducer

import (
	"context"
	"errors"
	"fmt"

	dumpcontext "github.com/greenmaskio/greenmask/v1/internal/common/dump/context"
	commondumpers "github.com/greenmaskio/greenmask/v1/internal/common/dump/dumpers"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/pipeline"
	"github.com/greenmaskio/greenmask/v1/internal/common/rawrecord"
	"github.com/greenmaskio/greenmask/v1/internal/common/record"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset"
	"github.com/greenmaskio/greenmask/v1/internal/common/tabledriver"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dump/streamers"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

var (
	errUnableToFindTableContext = fmt.Errorf("unable to find table context")
)

func newMysqlTableDriver(
	vc *validationcollector.Collector,
	table commonmodels.Table,
	columnsTypeOverride map[string]string,
) (commonininterfaces.TableDriver, error) {
	return tabledriver.New(vc, mysqldbmsdriver.New(), &table, columnsTypeOverride)
}

type TaskProducer struct {
	introspector commonininterfaces.Introspector
	tableConfigs []commonmodels.TableConfig
	registry     *registry.TransformerRegistry
	connConfig   mysqlmodels.ConnConfig
	st           storages.Storager
	s            subset.Subset
}

func New(
	i commonininterfaces.Introspector,
	tableConfigs []commonmodels.TableConfig,
	registry *registry.TransformerRegistry,
	connConfig mysqlmodels.ConnConfig,
	st storages.Storager,
) (*TaskProducer, error) {
	s, err := subset.NewSubset(i.GetCommonTables(), subset.DialectMySQL)
	if err != nil {
		return nil, fmt.Errorf("build subset queries: %w", err)
	}
	return &TaskProducer{
		introspector: i,
		tableConfigs: tableConfigs,
		registry:     registry,
		connConfig:   connConfig,
		st:           st,
		s:            s,
	}, nil
}

func (tp *TaskProducer) getTableContext(ctx context.Context, vc *validationcollector.Collector) ([]dumpcontext.TableContext, error) {
	p := dumpcontext.New(
		tp.introspector.GetCommonTables(),
		tp.s.GetTableQueries(),
		tp.tableConfigs,
		newMysqlTableDriver,
		tp.registry,
	)
	tableRuntimes, err := p.Build(ctx, vc)
	if err != nil {
		return nil, fmt.Errorf("produce table runtimes: %w", err)
	}
	return tableRuntimes, nil
}

func (tp *TaskProducer) initTableDumper(
	tableContext dumpcontext.TableContext, objectID commonmodels.TaskID,
) commonininterfaces.Dumper {
	tr := streamers.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tw := streamers.NewTableDataWriter(*tableContext.Table, tp.st, true)
	rawRecord := rawrecord.NewRawRecord(len(tableContext.Table.Columns), mysqldbmsdriver.NullValueSeq)
	r := record.NewRecord(rawRecord, tableContext.TableDriver)
	p := pipeline.NewTransformationPipeline(&tableContext)
	return commondumpers.NewTableDumper(objectID, tr, tw, r, p, tableContext.Table)
}

func (tp *TaskProducer) initTableRawDumper(
	tableContext dumpcontext.TableContext, objectID commonmodels.TaskID,
) commonininterfaces.Dumper {
	tr := streamers.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tw := streamers.NewTableDataWriter(*tableContext.Table, tp.st, true)
	return commondumpers.NewTableRawDumper(objectID, tr, tw, tableContext.Table)
}

func (tp *TaskProducer) Produce(
	ctx context.Context, vc *validationcollector.Collector,
) ([]commonininterfaces.Dumper, commonmodels.RestorationContext, error) {
	var objectID commonmodels.TaskID
	tablesContext, err := tp.getTableContext(ctx, vc)
	if err != nil {
		return nil, commonmodels.RestorationContext{}, fmt.Errorf("get table context: %w", err)
	}

	tableID2TaskID := make(map[commonmodels.ObjectID]commonmodels.TaskID)
	res := make([]commonininterfaces.Dumper, len(tablesContext))
	for i := range tablesContext {
		objectID++
		if tablesContext[i].HasTransformer() {
			res[i] = tp.initTableDumper(tablesContext[i], objectID)
		} else {
			res[i] = tp.initTableRawDumper(tablesContext[i], objectID)
		}
		tableID2TaskID[commonmodels.ObjectID(tablesContext[i].Table.ID)] = objectID
	}
	// TODO: Add scoring for tables so they have to be sorted by size.
	restorationContext, err := tp.buildRestorationContext(tableID2TaskID)
	if err != nil {
		return nil, commonmodels.RestorationContext{}, fmt.Errorf("get topologic order: %w", err)
	}

	return res, restorationContext, nil
}

func (tp *TaskProducer) getDependsOn(
	tableID2TaskID map[commonmodels.ObjectID]commonmodels.TaskID,
	tableID commonmodels.ObjectID,
) []commonmodels.TaskID {
	dependencies := tp.s.GetTableGraph().Graph[tableID]
	res := make([]commonmodels.TaskID, 0, len(dependencies))
	for _, dependency := range dependencies {
		dependentTableID, ok := tableID2TaskID[commonmodels.ObjectID(dependency.To().TableID())]
		if !ok {
			panic("table ID not found in dump ID map")
		}
		res = append(res, dependentTableID)
	}
	return res
}

func (tp *TaskProducer) buildRestorationContext(
	tableID2TaskID map[commonmodels.ObjectID]commonmodels.TaskID,
) (commonmodels.RestorationContext, error) {
	hasTopologicalOrder := true
	order, err := tp.s.GetTopologicalOrder()
	if err != nil {
		if errors.Is(err, commonmodels.ErrTableGraphHasCycles) {
			hasTopologicalOrder = false
		} else {
			return commonmodels.RestorationContext{}, fmt.Errorf("get topological order: %w", err)
		}
	}

	taskDependencies := make(map[commonmodels.TaskID][]commonmodels.TaskID)
	restorationOrder := make([]commonmodels.TaskID, len(order))
	for i, tableID := range order {
		taskID, ok := tableID2TaskID[commonmodels.ObjectID(tableID)]
		if !ok {
			panic("table ID not found in dump ID map")
		}
		restorationOrder[i] = taskID
		taskDependencies[taskID] = tp.getDependsOn(tableID2TaskID, commonmodels.ObjectID(tableID))
	}
	return commonmodels.RestorationContext{
		HasTopologicalOrder: hasTopologicalOrder,
		TaskDependencies:    taskDependencies,
		RestorationOrder:    restorationOrder,
	}, nil
}
