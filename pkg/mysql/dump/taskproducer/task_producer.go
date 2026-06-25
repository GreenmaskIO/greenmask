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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	dumpcontext "github.com/greenmaskio/greenmask/pkg/common/dump/context"
	"github.com/greenmaskio/greenmask/pkg/common/dump/dumpers"
	"github.com/greenmaskio/greenmask/pkg/common/pipeline"
	"github.com/greenmaskio/greenmask/pkg/common/rawrecord"
	"github.com/greenmaskio/greenmask/pkg/common/record"
	"github.com/greenmaskio/greenmask/pkg/common/subset"
	"github.com/greenmaskio/greenmask/pkg/common/tabledriver"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	tablestremers "github.com/greenmaskio/greenmask/pkg/mysql/dump/table"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
)

func newMysqlTableDriver(
	ctx context.Context,
	table core.Table,
	columnsTypeOverride map[string]string,
) (core.TableDriver, error) {
	return tabledriver.New(ctx, dbmsdriver.New(), &table, columnsTypeOverride)
}

type Option func(*DumpObjectPoducer) error

type DumpObjectPoducer struct {
	introspector          core.Introspector
	tableConfigs          []core.TableConfig
	registry              *registry.TransformerRegistry
	connConfig            mysqlmodels.ConnConfig
	st                    core.Storager
	txPool                *pool.ConsistentDumpTxPool
	subset                subset.Subset
	filter                core.TaskProducerFilter
	saveOriginal          bool
	rowLimit              int64
	compressionEnabled    bool
	compressionPgzip      bool
	transformedTablesOnly bool
	dumpFormat            core.DumpFormat
	hexBlob               bool
}

func WithFilter(
	filter core.TaskProducerFilter,
) func(*DumpObjectPoducer) error {
	return func(tp *DumpObjectPoducer) error {
		tp.filter = filter
		return nil
	}
}

func WithSaveOriginalData() Option {
	return func(tp *DumpObjectPoducer) error {
		tp.saveOriginal = true
		return nil
	}
}

func WithRowLimit(limit int64) Option {
	return func(tp *DumpObjectPoducer) error {
		if limit < 0 {
			return fmt.Errorf("row limit cannot be negative: %d", limit)
		}
		tp.rowLimit = limit
		return nil
	}
}

func WithCompressionEnabled() Option {
	return func(tp *DumpObjectPoducer) error {
		tp.compressionEnabled = true
		return nil
	}
}

func WithCompressionPgzip() Option {
	return func(tp *DumpObjectPoducer) error {
		tp.compressionPgzip = true
		return nil
	}
}

func WithTransformedTablesOnly() Option {
	return func(tp *DumpObjectPoducer) error {
		tp.transformedTablesOnly = true
		return nil
	}
}

func WithDumpFormat(format core.DumpFormat) Option {
	return func(tp *DumpObjectPoducer) error {
		if format != "" {
			tp.dumpFormat = format
		}
		return nil
	}
}

func WithHexBlob() Option {
	return func(tp *DumpObjectPoducer) error {
		tp.hexBlob = true
		return nil
	}
}

func enrichWithSubsetQueries(tables []core.Table, tableConfigs []core.TableConfig) []core.Table {
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
	i core.Introspector,
	tableConfigs []core.TableConfig,
	registry *registry.TransformerRegistry,
	connConfig mysqlmodels.ConnConfig,
	st core.Storager,
	txPool *pool.ConsistentDumpTxPool,
	opts ...Option,
) (*DumpObjectPoducer, error) {
	tables := enrichWithSubsetQueries(i.GetCommonTables(), tableConfigs)
	s, err := subset.NewSubset(tables, subset.DialectMySQL)
	if err != nil {
		return nil, fmt.Errorf("build subset queries: %w", err)
	}
	res := &DumpObjectPoducer{
		introspector: i,
		tableConfigs: tableConfigs,
		registry:     registry,
		connConfig:   connConfig,
		st:           st,
		subset:       s,
		txPool:       txPool,
	}
	for i, opt := range opts {
		if err := opt(res); err != nil {
			return nil, fmt.Errorf("apply task producer option %d: %w", i, err)
		}
	}
	return res, nil
}

func (tp *DumpObjectPoducer) getTableContext(ctx context.Context) ([]dumpcontext.TableDumpContextPayload, error) {
	tables := tp.introspector.GetCommonTables()
	queries := tp.subset.GetTableQueries()
	allowedTables := make([]core.Table, 0, len(tables))
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

func (tp *DumpObjectPoducer) initTableDumper(
	tableContext dumpcontext.TableDumpContextPayload, objectID core.TaskID,
) (core.ObjectDumper, error) {
	tr := tablestremers.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tr.SetTxPool(tp.txPool)
	tw := tablestremers.NewTableDataWriter(*tableContext.Table, tp.st,
		tablestremers.WithCompression(tp.compressionEnabled),
		tablestremers.WithPgzip(tp.compressionPgzip),
		tablestremers.WithFormat(tp.dumpFormat),
		tablestremers.WithHexBlob(tp.hexBlob),
	)
	rawRecord := rawrecord.NewRawRecord(len(tableContext.Table.Columns), core.NullValueSeq)
	r := record.NewRecord(rawRecord, tableContext.TableDriver)
	p := pipeline.NewTransformationPipeline(&tableContext)
	var opts []dumpers.TableDumperOption
	if tp.saveOriginal {
		opts = append(opts, dumpers.WithSaveOriginalData())
	}
	if tp.rowLimit > 0 {
		opts = append(opts, dumpers.WithRowLimit(tp.rowLimit))
	}
	dumper, err := dumpers.NewTableDumper(objectID, tr, tw, r, p, tableContext.Table, opts...)
	if err != nil {
		return nil, fmt.Errorf("create table dumper: %w", err)
	}
	return dumper, nil
}

func (tp *DumpObjectPoducer) initTableRawDumper(
	tableContext dumpcontext.TableDumpContextPayload, objectID core.TaskID,
) core.ObjectDumper {
	tr := tablestremers.NewTableDataReader(tableContext.Table, tp.connConfig, tableContext.Query)
	tr.SetTxPool(tp.txPool)
	tw := tablestremers.NewTableDataWriter(*tableContext.Table, tp.st,
		tablestremers.WithCompression(tp.compressionEnabled),
		tablestremers.WithPgzip(tp.compressionPgzip),
		tablestremers.WithFormat(tp.dumpFormat),
		tablestremers.WithHexBlob(tp.hexBlob),
	)
	return dumpers.NewTableRawDumper(objectID, tr, tw, tableContext.Table)
}

func (tp *DumpObjectPoducer) Produce(
	ctx context.Context,
) ([]core.ObjectDumper, core.RestorationContext, error) {
	var taskID core.TaskID
	tablesContext, err := tp.getTableContext(ctx)
	if err != nil {
		return nil, core.RestorationContext{}, fmt.Errorf("get table context: %w", err)
	}

	tableID2TaskID := make(map[core.ObjectID]core.TaskID)
	tableIDAffectedColumns := make(map[core.ObjectID][]int)
	res := make([]core.ObjectDumper, 0, len(tablesContext))
	for i := range tablesContext {
		if !tp.filter.IsDataAllowed(*tablesContext[i].Table) || !tablesContext[i].Table.NeedDumpData {
			continue
		}
		if !tablesContext[i].HasTransformer() && tp.transformedTablesOnly {
			// Skip non transformed tables for validate command.
			continue
		}
		taskID++
		if tablesContext[i].HasTransformer() {
			dumper, err := tp.initTableDumper(tablesContext[i], taskID)
			if err != nil {
				return nil, core.RestorationContext{}, fmt.Errorf("init table dumper: %w", err)
			}
			res = append(res, dumper)
		} else {
			res = append(res, tp.initTableRawDumper(tablesContext[i], taskID))
		}
		tableID2TaskID[core.ObjectID(tablesContext[i].Table.ID)] = taskID
		affectedColumns := tablesContext[i].GetAffectedColumns()
		tableIDAffectedColumns[core.ObjectID(tablesContext[i].Table.ID)] = affectedColumns
	}
	// TODO: Add scoring for tables so they have to be sorted by size.
	restorationContext, err := tp.buildRestorationContext(ctx, tableID2TaskID, tableIDAffectedColumns)
	if err != nil {
		return nil, core.RestorationContext{}, fmt.Errorf("get topologic order: %w", err)
	}

	return res, restorationContext, nil
}

func (tp *DumpObjectPoducer) getDependsOn(
	ctx context.Context,
	tableID2TaskID map[core.ObjectID]core.TaskID,
	tableID core.ObjectID,
) []core.TaskID {
	dependencies := tp.subset.GetTableGraph().Graph[tableID]
	res := make([]core.TaskID, 0, len(dependencies))
	for _, dependency := range dependencies {
		dependentTableID, ok := tableID2TaskID[core.ObjectID(dependency.To().TableID())]
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

func (tp *DumpObjectPoducer) buildRestorationContext(
	ctx context.Context,
	tableID2TaskID map[core.ObjectID]core.TaskID,
	tableIDToAffectedColumn map[core.ObjectID][]int,
) (core.RestorationContext, error) {
	hasTopologicalOrder := true
	order, err := tp.subset.GetTopologicalOrder()
	if err != nil {
		if errors.Is(err, core.ErrTableGraphHasCycles) {
			hasTopologicalOrder = false
		} else {
			return core.RestorationContext{}, fmt.Errorf("get topological order: %w", err)
		}
	}

	taskDependencies := make(map[core.TaskID][]core.TaskID)
	restorationOrder := make([]core.TaskID, len(order))
	for i, tableID := range order {
		taskID, ok := tableID2TaskID[core.ObjectID(tableID)]
		if !ok {
			// TODO: revise it later, maybe we should return an error here
			log.Ctx(ctx).Debug().
				Int("TableID", tableID).
				Str("Info", "most likely table was filtered out").
				Msg("table is not found in the task ID map")
			continue
		}
		restorationOrder[i] = taskID
		taskDependencies[taskID] = tp.getDependsOn(ctx, tableID2TaskID, core.ObjectID(tableID))
	}
	return core.RestorationContext{
		HasTopologicalOrder:      hasTopologicalOrder,
		TaskDependencies:         taskDependencies,
		RestorationOrder:         restorationOrder,
		TableIDToAffectedColumns: tableIDToAffectedColumn,
	}, nil
}
