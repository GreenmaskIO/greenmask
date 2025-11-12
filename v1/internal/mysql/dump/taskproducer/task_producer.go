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

package taskproducer

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

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
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/dump/streamers"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
)

func newMysqlTableDriver(
	ctx context.Context,
	table commonmodels.Table,
	columnsTypeOverride map[string]string,
) (commonininterfaces.TableDriver, error) {
	return tabledriver.New(ctx, mysqldbmsdriver.New(), &table, columnsTypeOverride)
}

type Option func(*TaskProducer) error

type TaskProducer struct {
	introspector       commonininterfaces.Introspector
	tableConfigs       []commonmodels.TableConfig
	registry           *registry.TransformerRegistry
	connConfig         mysqlmodels.ConnConfig
	st                 commonininterfaces.Storager
	subset             subset.Subset
	filter             commonmodels.TaskProducerFilter
	saveOriginal       bool
	rowLimit           int64
	compressionEnabled bool
	compressionPgzip   bool
}

func WithFilter(
	filter commonmodels.TaskProducerFilter,
) func(*TaskProducer) error {
	return func(tp *TaskProducer) error {
		tp.filter = filter
		return nil
	}
}

func WithSaveOriginalData() Option {
	return func(tp *TaskProducer) error {
		tp.saveOriginal = true
		return nil
	}
}

func WithRowLimit(limit int64) Option {
	return func(tp *TaskProducer) error {
		if limit < 0 {
			return fmt.Errorf("row limit cannot be negative: %d", limit)
		}
		tp.rowLimit = limit
		return nil
	}
}

func WithCompressionEnabled() Option {
	return func(tp *TaskProducer) error {
		tp.compressionEnabled = true
		return nil
	}
}

func WithCompressionPgzip() Option {
	return func(tp *TaskProducer) error {
		tp.compressionPgzip = true
		return nil
	}
}

func enrichWithSubsetQueries(tables []commonmodels.Table, tableConfigs []commonmodels.TableConfig) []commonmodels.Table {
	for _, tc := range tableConfigs {
		if len(tc.SubsetConds) > 0 {
			for i := range tables {
				if tables[i].Schema == tc.Schema && tables[i].Name == tc.Name {
					tables[i].SubsetConditions = tc.SubsetConds
					break
				}
			}
		}
	}
	return tables
}

func New(
	i commonininterfaces.Introspector,
	tableConfigs []commonmodels.TableConfig,
	registry *registry.TransformerRegistry,
	connConfig mysqlmodels.ConnConfig,
	st commonininterfaces.Storager,
	opts ...Option,
) (*TaskProducer, error) {
	tables := enrichWithSubsetQueries(i.GetCommonTables(), tableConfigs)
	s, err := subset.NewSubset(tables, subset.DialectMySQL)
	if err != nil {
		return nil, fmt.Errorf("build subset queries: %w", err)
	}
	res := &TaskProducer{
		introspector: i,
		tableConfigs: tableConfigs,
		registry:     registry,
		connConfig:   connConfig,
		st:           st,
		subset:       s,
	}
	for i, opt := range opts {
		if err := opt(res); err != nil {
			return nil, fmt.Errorf("apply task producer option %d: %w", i, err)
		}
	}
	return res, nil
}

func (tp *TaskProducer) getTableContext(ctx context.Context) ([]dumpcontext.TableContext, error) {
	tables := tp.introspector.GetCommonTables()
	queries := tp.subset.GetTableQueries()
	allowedTables := make([]commonmodels.Table, 0, len(tables))
	allowedTableQueries := make([]string, 0, len(tables))
	for i := range tables {
		if tp.filter.IsAllowed(tables[i]) {
			allowedTables = append(allowedTables, tables[i])
			allowedTableQueries = append(allowedTableQueries, queries[i])
		}
	}
	p := dumpcontext.New(
		allowedTables,
		allowedTableQueries,
		tp.tableConfigs,
		newMysqlTableDriver,
		tp.registry,
	)
	tableRuntimes, err := p.Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("build table context: %w", err)
	}
	return tableRuntimes, nil
}

func (tp *TaskProducer) initTableDumper(
	tableContext dumpcontext.TableContext, objectID commonmodels.TaskID,
) (commonininterfaces.Dumper, error) {
	tr := streamers.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tw := streamers.NewTableDataWriter(*tableContext.Table, tp.st, streamers.CompressionSettings{
		Enabled: tp.compressionEnabled,
		Pgzip:   tp.compressionPgzip,
	})
	rawRecord := rawrecord.NewRawRecord(len(tableContext.Table.Columns), mysqldbmsdriver.NullValueSeq)
	r := record.NewRecord(rawRecord, tableContext.TableDriver)
	p := pipeline.NewTransformationPipeline(&tableContext)
	var opts []commondumpers.TableDumperOption
	if tp.saveOriginal {
		opts = append(opts, commondumpers.WithSaveOriginalData())
	}
	if tp.rowLimit > 0 {
		opts = append(opts, commondumpers.WithRowLimit(tp.rowLimit))
	}
	dumper, err := commondumpers.NewTableDumper(objectID, tr, tw, r, p, tableContext.Table, opts...)
	if err != nil {
		return nil, fmt.Errorf("create table dumper: %w", err)
	}
	return dumper, nil
}

func (tp *TaskProducer) initTableRawDumper(
	tableContext dumpcontext.TableContext, objectID commonmodels.TaskID,
) commonininterfaces.Dumper {
	tr := streamers.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tw := streamers.NewTableDataWriter(*tableContext.Table, tp.st, streamers.CompressionSettings{
		Enabled: tp.compressionEnabled,
		Pgzip:   tp.compressionPgzip,
	})
	return commondumpers.NewTableRawDumper(objectID, tr, tw, tableContext.Table)
}

func (tp *TaskProducer) Produce(
	ctx context.Context,
) ([]commonininterfaces.Dumper, commonmodels.RestorationContext, error) {
	var taskID commonmodels.TaskID
	tablesContext, err := tp.getTableContext(ctx)
	if err != nil {
		return nil, commonmodels.RestorationContext{}, fmt.Errorf("get table context: %w", err)
	}

	tableID2TaskID := make(map[commonmodels.ObjectID]commonmodels.TaskID)
	tableIDAffectedColumns := make(map[commonmodels.ObjectID][]int)
	res := make([]commonininterfaces.Dumper, len(tablesContext))
	for i := range tablesContext {
		taskID++
		if tablesContext[i].HasTransformer() {
			res[i], err = tp.initTableDumper(tablesContext[i], taskID)
			if err != nil {
				return nil, commonmodels.RestorationContext{}, fmt.Errorf("init table dumper: %w", err)
			}
		} else {
			res[i] = tp.initTableRawDumper(tablesContext[i], taskID)
		}
		tableID2TaskID[commonmodels.ObjectID(tablesContext[i].Table.ID)] = taskID
		affectedColumns := tablesContext[i].GetAffectedColumns()
		tableIDAffectedColumns[commonmodels.ObjectID(tablesContext[i].Table.ID)] = affectedColumns
	}
	// TODO: Add scoring for tables so they have to be sorted by size.
	restorationContext, err := tp.buildRestorationContext(ctx, tableID2TaskID, tableIDAffectedColumns)
	if err != nil {
		return nil, commonmodels.RestorationContext{}, fmt.Errorf("get topologic order: %w", err)
	}

	return res, restorationContext, nil
}

func (tp *TaskProducer) getDependsOn(
	ctx context.Context,
	tableID2TaskID map[commonmodels.ObjectID]commonmodels.TaskID,
	tableID commonmodels.ObjectID,
) []commonmodels.TaskID {
	dependencies := tp.subset.GetTableGraph().Graph[tableID]
	res := make([]commonmodels.TaskID, 0, len(dependencies))
	for _, dependency := range dependencies {
		dependentTableID, ok := tableID2TaskID[commonmodels.ObjectID(dependency.To().TableID())]
		if !ok {
			// TODO: revise it later, maybe we should return an error here
			log.Ctx(ctx).Debug().
				Int("TableID", int(tableID)).
				Str("Info", "most likely table was filtered out").
				Msg("table dependency not found in the map")
			continue
		}
		res = append(res, dependentTableID)
	}
	return res
}

func (tp *TaskProducer) buildRestorationContext(
	ctx context.Context,
	tableID2TaskID map[commonmodels.ObjectID]commonmodels.TaskID,
	tableIDToAffectedColumn map[commonmodels.ObjectID][]int,
) (commonmodels.RestorationContext, error) {
	hasTopologicalOrder := true
	order, err := tp.subset.GetTopologicalOrder()
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
			// TODO: revise it later, maybe we should return an error here
			log.Ctx(ctx).Debug().
				Int("TableID", tableID).
				Str("Info", "most likely table was filtered out").
				Msg("table is not found in the task ID map")
			continue
		}
		restorationOrder[i] = taskID
		taskDependencies[taskID] = tp.getDependsOn(ctx, tableID2TaskID, commonmodels.ObjectID(tableID))
	}
	return commonmodels.RestorationContext{
		HasTopologicalOrder:      hasTopologicalOrder,
		TaskDependencies:         taskDependencies,
		RestorationOrder:         restorationOrder,
		TableIDToAffectedColumns: tableIDToAffectedColumn,
	}, nil
}
