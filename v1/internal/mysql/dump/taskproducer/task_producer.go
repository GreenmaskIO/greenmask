package taskproducer

import (
	"context"
	"fmt"

	context2 "github.com/greenmaskio/greenmask/v1/internal/common/dump/context"
	dumpers2 "github.com/greenmaskio/greenmask/v1/internal/common/dump/dumpers"
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
	streamers2 "github.com/greenmaskio/greenmask/v1/internal/mysql/dump/streamers"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
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
}

func New(
	i commonininterfaces.Introspector,
	tableConfigs []commonmodels.TableConfig,
	registry *registry.TransformerRegistry,
	connConfig mysqlmodels.ConnConfig,
	st storages.Storager,
) *TaskProducer {
	return &TaskProducer{
		introspector: i,
		tableConfigs: tableConfigs,
		registry:     registry,
		connConfig:   connConfig,
		st:           st,
	}
}
func (tp *TaskProducer) getTableContext(ctx context.Context, vc *validationcollector.Collector) ([]context2.TableContext, error) {
	s, err := subset.NewSubset(tp.introspector.GetCommonTables(), subset.DialectMySQL)
	if err != nil {
		return nil, fmt.Errorf("build subset queries: %w", err)
	}

	p := context2.New(
		tp.introspector.GetCommonTables(),
		s.GetTableQueries(),
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
	tableContext context2.TableContext,
) commonininterfaces.Dumper {
	tr := streamers2.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tw := streamers2.NewTableDataWriter(*tableContext.Table, tp.st, true)
	rawRecord := rawrecord.NewRawRecord(len(tableContext.Table.Columns), mysqldbmsdriver.NullValueSeq)
	r := record.NewRecord(rawRecord, tableContext.TableDriver)
	p := pipeline.NewTransformationPipeline(&tableContext)
	return dumpers2.NewTableDumper(tr, tw, r, p)
}

func (tp *TaskProducer) initTableRawDumper(
	tableContext context2.TableContext,
) commonininterfaces.Dumper {
	tr := streamers2.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tw := streamers2.NewTableDataWriter(*tableContext.Table, tp.st, true)
	return dumpers2.NewTableRawDumper(tr, tw)
}

func (tp *TaskProducer) Generate(
	ctx context.Context, vc *validationcollector.Collector,
) ([]commonininterfaces.Dumper, error) {
	tablesContext, err := tp.getTableContext(ctx, vc)
	if err != nil {
		return nil, fmt.Errorf("get table context: %w", err)
	}
	res := make([]commonininterfaces.Dumper, len(tablesContext))
	for i := range tablesContext {
		if tablesContext[i].HasTransformer() {
			res[i] = tp.initTableDumper(tablesContext[i])
		} else {
			res[i] = tp.initTableRawDumper(tablesContext[i])
		}
	}
	// TODO: Add scoring for tables so they have to be sorted by size.

	return res, nil
}
