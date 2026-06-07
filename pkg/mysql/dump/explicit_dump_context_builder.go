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

package dump

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/tablebuilder"
	"github.com/greenmaskio/greenmask/pkg/common/tabledriver"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	"github.com/rs/zerolog/log"
)

var _ core.ExplicitDumpContextBuilder = (*ExplicitDumpContextBuilder)(nil)

func newMysqlTableDriver(
	ctx context.Context,
	table core.Table,
	columnsTypeOverride map[string]string,
) (core.TableDriver, error) {
	return tabledriver.New(ctx, dbmsdriver.New(), &table, columnsTypeOverride)
}

// ExplicitDumpContextBuilder builds the dump context from explicit configuration.
type ExplicitDumpContextBuilder struct{}

func validateSupportedKinds(kinds []core.ObjectKind) error {
	for _, kind := range kinds {
		if kind.IsDataSection() {
			if kind != core.ObjectKindMysqlTable {
				return fmt.Errorf("%w %q: MySQL dump only supports tables as data section", errUnsupportedObjectKind, kind)
			}
		} else {
			switch kind {
			case core.ObjectKindMysqlDatabase, core.ObjectKindMysqlSchema:
			default:
				return fmt.Errorf("%w %q: MySQL dump only supports mysql.database and mysql.schema as schema sections", errUnsupportedObjectKind, kind)
			}
		}
	}
	return nil
}

func (b *ExplicitDumpContextBuilder) BuildDumpContext(
	ctx context.Context,
	in core.ExplicitDumpContextInput,
) (core.DumpContext, error) {
	// You have tables and other objects definitions. You need to initialize transformers if
	// required and the other parts.

	if err := validateSupportedKinds(in.IntrospectionResult.GetKinds()); err != nil {
		return core.DumpContext{}, err
	}

	seq := new(core.TaskIDSequence)

	dumpObjectSpecs, err := b.buildDumpObjectSpecs(ctx, in, seq)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("build dump objects: %w", err)
	}

	schemaDumpSpecs, err := b.buildSchemaDumpSpecs(seq)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("build schema dump specs: %w", err)
	}

	return core.DumpContext{
		DumpObjectSpecs: dumpObjectSpecs,
		SchemaDumpSpecs: schemaDumpSpecs,
	}, nil
}

func payloadToTableDefinition(obj core.Object) (core.Table, error) {
	if obj.Kind != core.ObjectKindMysqlTable {
		return core.Table{}, fmt.Errorf("unknown kind %s", obj.Kind)
	}
	res, ok := obj.Payload.(core.Table)
	if !ok {
		return core.Table{}, fmt.Errorf("unknown payload kind %+v", obj.Payload)
	}
	return res, nil
}

func (b *ExplicitDumpContextBuilder) initTable(
	ctx context.Context,
	tableConfig *core.TableConfig,
	subsetQuery string,
	obj core.Object,
	registry core.TransformerRegistry,
	seq *core.TaskIDSequence,
) (core.ObjectDumpSpec, error) {
	table, err := payloadToTableDefinition(obj)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("get table definition: %w", err)
	}
	ctx = log.Ctx(ctx).With().
		Str(core.MetaKeyTableSchema, table.Schema).
		Str(core.MetaKeyTableName, table.Name).
		Logger().WithContext(ctx)

	tableDriver, err := newMysqlTableDriver(ctx, table, tableConfig.ColumnsTypeOverride)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("init table driver: %w", err)
	}
	dumpQuery := subsetQuery
	if dumpQuery == "" && tableConfig.Query != "" {
		dumpQuery = tableConfig.Query
	}
	tableCondition, err := tablebuilder.CompileTableCondition(ctx, table, tableConfig)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("compile table condition: %w", err)
	}
	transformerContext, err := tablebuilder.InitTableTransformers(ctx, tableDriver, tableConfig.Transformers, registry)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("init transformation runtimes: %w", err)
	}
	payload := transformercontext.TableDumpContext{
		Table:              &table,
		Condition:          tableCondition,
		TransformerContext: transformerContext,
		Query:              dumpQuery,
		TableDriver:        tableDriver,
	}

	return core.ObjectDumpSpec{
		TaskID:   seq.Next(),
		Kind:     core.ObjectKindMysqlTable,
		ObjectID: obj.ID,
		Name:     obj.Name,
		Payload:  payload,
	}, nil
}

// buildDumpObjectSpecs creates an ObjectDumpSpec for every table object found in
// the introspection result. Mode is set to transformed when the table has
// transformer configuration, raw otherwise.
func (b *ExplicitDumpContextBuilder) buildDumpObjectSpecs(
	ctx context.Context,
	in core.ExplicitDumpContextInput,
	seq *core.TaskIDSequence,
) ([]core.ObjectDumpSpec, error) {
	tableObjects, ok := in.IntrospectionResult.KindsMap[core.ObjectKindMysqlTable]
	if !ok {
		log.Ctx(ctx).Debug().Msg("no table config for dump objects")
		return nil, nil
	}
	tableDumpContextPayloads := make([]core.ObjectDumpSpec, len(tableObjects))
	ctx, err := utils.WithSaltFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("set salt: %w", err)
	}
	for i := range tableObjects {
		table, err := payloadToTableDefinition(tableObjects[i])
		if err != nil {
			return nil, fmt.Errorf("build table dump specs: %w", err)
		}
		tableConfig := tablebuilder.GetTableConfig(in.TableConfigs, table)
		subsetQuery := tablebuilder.GetTableSubsetQuery(in.Subset, tableObjects[i])

		res, err := b.initTable(ctx, tableConfig, subsetQuery, tableObjects[i],
			in.TransformerRegistry, seq)
		if err != nil {
			return nil, fmt.Errorf("init table %s: %w", tableObjects[i].Name, err)
		}
		tableDumpContextPayloads = append(tableDumpContextPayloads, res)
	}
	return tableDumpContextPayloads, nil
}

// buildSchemaDumpSpecs returns specs for the two MySQL schema sections:
// pre-data (DDL: CREATE TABLE statements) and post-data (indexes, triggers, etc.).
func (b *ExplicitDumpContextBuilder) buildSchemaDumpSpecs(seq *core.TaskIDSequence) ([]core.SchemaDumpSpec, error) {
	return []core.SchemaDumpSpec{
		{
			TaskID:       seq.Next(),
			Kind:         core.ObjectKindMysqlTable,
			Name:         string(core.SchemaDumpKindMySQLPreData),
			NeedDumpData: true,
			Payload:      nil,
		},
		{
			TaskID:       seq.Next(),
			Kind:         core.ObjectKindMysqlTable,
			Name:         string(core.SchemaDumpKindMySQLPostData),
			NeedDumpData: true,
			Payload:      nil,
		},
	}, nil
}
