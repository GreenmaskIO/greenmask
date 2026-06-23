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

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/tablebuilder"
	"github.com/greenmaskio/greenmask/pkg/common/tabledriver"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	schemadump "github.com/greenmaskio/greenmask/pkg/mysql/dump/factory/schema"
)

var _ core.ExplicitDumpContextBuilder = (*ExplicitDumpContextBuilder)(nil)

// tableInitDeps abstracts the per-table collaborators required to build an
// ObjectDumpSpec: constructing the table driver, compiling the table-level
// condition, and initialising the transformer runtimes.
//
// It is an interface so the builder can be unit-tested with stubs instead of
// constructing a real DBMS driver and a populated transformer registry.
type tableInitDeps interface {
	NewTableDriver(ctx context.Context, table core.Table, columnsTypeOverride map[string]string) (core.TableDriver, error)
	CompileCondition(ctx context.Context, table core.Table, tableConfig *core.TableConfig) (core.CondEvaluator, error)
	InitTransformers(ctx context.Context, driver core.TableDriver, configs []core.TransformerConfig, registry core.TransformerRegistry) ([]core.TransformerContexter, error)
}

// defaultTableInitDeps is the production implementation of tableInitDeps, wired
// to the real MySQL driver and the shared tablebuilder helpers.
type defaultTableInitDeps struct{}

func (defaultTableInitDeps) NewTableDriver(
	ctx context.Context,
	table core.Table,
	columnsTypeOverride map[string]string,
) (core.TableDriver, error) {
	return tabledriver.New(ctx, dbmsdriver.New(), &table, columnsTypeOverride)
}

func (defaultTableInitDeps) CompileCondition(
	ctx context.Context,
	table core.Table,
	tableConfig *core.TableConfig,
) (core.CondEvaluator, error) {
	return tablebuilder.CompileTableCondition(ctx, table, tableConfig)
}

func (defaultTableInitDeps) InitTransformers(
	ctx context.Context,
	driver core.TableDriver,
	configs []core.TransformerConfig,
	registry core.TransformerRegistry,
) ([]core.TransformerContexter, error) {
	return tablebuilder.InitTableTransformers(ctx, driver, configs, registry)
}

// ExplicitDumpContextBuilder builds the dump context from explicit configuration.
type ExplicitDumpContextBuilder struct {
	deps     tableInitDeps
	registry core.TransformerRegistry
}

// NewExplicitDumpContextBuilder builds an ExplicitDumpContextBuilder wired to the
// production table-init collaborators. registry resolves transformer
// configurations into runtime transformers.
func NewExplicitDumpContextBuilder(registry core.TransformerRegistry) *ExplicitDumpContextBuilder {
	return &ExplicitDumpContextBuilder{deps: defaultTableInitDeps{}, registry: registry}
}

func validateSupportedKinds(kinds []core.ObjectKind) error {
	for _, kind := range kinds {
		if kind.IsDataSection() {
			if kind != core.ObjectKindMysqlTable {
				return fmt.Errorf("%w %q: MySQL dump only supports tables as data section", errUnsupportedObjectKind, kind)
			}
		} else {
			switch kind {
			case core.ObjectKindMysqlDatabase:
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

	schemaDumpSpecs, err := b.buildSchemaDumpSpecs(ctx, in, seq)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("build schema dump specs: %w", err)
	}

	databases, err := schemaDumpDatabases(in)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("collect source databases: %w", err)
	}

	return core.DumpContext{
		DumpObjectSpecs: dumpObjectSpecs,
		SchemaDumpSpecs: schemaDumpSpecs,
		Source:          mysqlSourceSpec(databases, in.IntrospectionResult.Version),
	}, nil
}

func payloadToTableDefinition(obj core.Object) (core.Table, error) {
	if obj.Kind != core.ObjectKindMysqlTable {
		return core.Table{}, fmt.Errorf("unknown kind %s", obj.Kind)
	}
	// The introspection payload is either a common table or an engine-specific
	// type that converts itself via ToCommonTable (e.g. *mysqlmodels.Table).
	switch p := obj.Payload.(type) {
	case core.Table:
		return p, nil
	case *core.Table:
		if p == nil {
			return core.Table{}, fmt.Errorf("object %q: nil table payload", obj.Name)
		}
		return *p, nil
	case interface{ ToCommonTable() core.Table }:
		return p.ToCommonTable(), nil
	default:
		return core.Table{}, fmt.Errorf("unsupported table payload type %T", obj.Payload)
	}
}

func (b *ExplicitDumpContextBuilder) initTable(
	ctx context.Context,
	tableConfig *core.TableConfig,
	subsetQuery string,
	obj core.Object,
	registry core.TransformerRegistry,
	seq *core.TaskIDSequence,
	compression core.Compression,
) (core.ObjectDumpSpec, error) {
	table, err := payloadToTableDefinition(obj)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("get table definition: %w", err)
	}
	ctx = log.Ctx(ctx).With().
		Str(core.MetaKeyTableSchema, table.Schema).
		Str(core.MetaKeyTableName, table.Name).
		Logger().WithContext(ctx)

	// No user config for this table — raw dump with no transformations and no
	// driver: a driver is only required to initialise transformers.
	if tableConfig == nil {
		return core.ObjectDumpSpec{
			TaskID:   seq.Next(),
			Kind:     core.ObjectKindMysqlTable,
			ObjectID: obj.ID,
			Name:     obj.Name,
			Identity: mysqlTableIdentity(table.Schema, table.Name),
			Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
			Mode:     core.DumpModeRaw,
			Payload: transformercontext.TableDumpContext{
				ColumnKind:  core.EntityKindMysqlColumn,
				Table:       &table,
				Query:       subsetQuery,
				Compression: compression,
			},
		}, nil
	}

	dumpQuery := subsetQuery
	if dumpQuery == "" && tableConfig.Query != "" {
		dumpQuery = tableConfig.Query
	}
	tableCondition, err := b.deps.CompileCondition(ctx, table, tableConfig)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("compile table condition: %w", err)
	}

	// Without transformers there is nothing to drive, so skip building the table
	// driver entirely and emit a raw spec (the table-level condition is still
	// honoured).
	if len(tableConfig.Transformers) == 0 {
		return core.ObjectDumpSpec{
			TaskID:   seq.Next(),
			Kind:     core.ObjectKindMysqlTable,
			ObjectID: obj.ID,
			Name:     obj.Name,
			Identity: mysqlTableIdentity(table.Schema, table.Name),
			Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
			Mode:     core.DumpModeRaw,
			Payload: transformercontext.TableDumpContext{
				ColumnKind:  core.EntityKindMysqlColumn,
				Table:       &table,
				Condition:   tableCondition,
				Query:       dumpQuery,
				Compression: compression,
			},
		}, nil
	}

	tableDriver, err := b.deps.NewTableDriver(ctx, table, tableConfig.ColumnsTypeOverride)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("init table driver: %w", err)
	}
	transformerContext, err := b.deps.InitTransformers(ctx, tableDriver, tableConfig.Transformers, registry)
	if err != nil {
		return core.ObjectDumpSpec{}, fmt.Errorf("init transformation runtimes: %w", err)
	}
	return core.ObjectDumpSpec{
		TaskID:   seq.Next(),
		Kind:     core.ObjectKindMysqlTable,
		ObjectID: obj.ID,
		Name:     obj.Name,
		Identity: mysqlTableIdentity(table.Schema, table.Name),
		Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
		Mode:     core.DumpModeTransformed,
		Payload: transformercontext.TableDumpContext{
			ColumnKind:         core.EntityKindMysqlColumn,
			Table:              &table,
			Condition:          tableCondition,
			TransformerContext: transformerContext,
			Query:              dumpQuery,
			TableDriver:        tableDriver,
			Compression:        compression,
		},
	}, nil
}

// buildDumpObjectSpecs creates an ObjectDumpSpec for every table object found in
// the introspection result that is present in AllowedObjects (or all tables if
// AllowedObjects is empty). Mode is set to transformed when the table has
// transformer configuration, raw otherwise.
func (b *ExplicitDumpContextBuilder) buildDumpObjectSpecs(
	ctx context.Context,
	in core.ExplicitDumpContextInput,
	seq *core.TaskIDSequence,
) ([]core.ObjectDumpSpec, error) {
	tableObjects := in.IntrospectionResult.KindsMap[core.ObjectKindMysqlTable]
	if len(tableObjects) == 0 {
		log.Ctx(ctx).Debug().Msg("no table objects to dump")
		return nil, nil
	}

	allowed, filterActive := tableAllowedFilter(in)

	ctx, err := utils.WithSaltFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("set salt: %w", err)
	}
	// Table data files use the same output compression as the schema dump,
	// derived from the run config.
	compression := mysqldumpOutputOptions(in.Config)

	var tableDumpContextPayloads []core.ObjectDumpSpec
	for i := range tableObjects {
		if filterActive {
			if _, ok := allowed[tableObjects[i].ID]; !ok {
				log.Ctx(ctx).Debug().
					Str("ObjectKind", string(tableObjects[i].Kind)).
					Str("ObjectName", tableObjects[i].Name).
					Int("ObjectID", int(tableObjects[i].ID)).
					Msg("skipping table dump object: filtered out by allowed objects")
				continue
			}
		}
		table, err := payloadToTableDefinition(tableObjects[i])
		if err != nil {
			return nil, fmt.Errorf("build table dump specs: %w", err)
		}
		tableConfig := tablebuilder.GetTableConfig(in.TableConfigs, table)
		subsetQuery := tablebuilder.GetTableSubsetQuery(in.Subset, tableObjects[i])

		res, err := b.initTable(ctx, tableConfig, subsetQuery, tableObjects[i],
			b.registry, seq, compression)
		if err != nil {
			return nil, fmt.Errorf("init table %s: %w", tableObjects[i].Name, err)
		}
		tableDumpContextPayloads = append(tableDumpContextPayloads, res)
	}
	return tableDumpContextPayloads, nil
}

// tableAllowedFilter returns the set of allowed table ObjectIDs and whether a
// filter is active. A nil/empty AllowedObjects entry means no filter is active
// and every table is allowed (see core.ObjectFilterResult).
func tableAllowedFilter(in core.ExplicitDumpContextInput) (allowed map[core.ObjectID]struct{}, active bool) {
	ids := in.AllowedObjects[core.ObjectKindMysqlTable]
	if len(ids) == 0 {
		return nil, false
	}
	allowed = make(map[core.ObjectID]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}
	return allowed, true
}

// allowedTableObjects returns the MySQL table objects from the introspection
// result that participate in the dump.
func allowedTableObjects(in core.ExplicitDumpContextInput) []core.Object {
	tableObjects := in.IntrospectionResult.KindsMap[core.ObjectKindMysqlTable]
	allowed, active := tableAllowedFilter(in)
	if !active {
		return tableObjects
	}
	res := make([]core.Object, 0, len(tableObjects))
	for i := range tableObjects {
		if _, ok := allowed[tableObjects[i].ID]; ok {
			res = append(res, tableObjects[i])
		}
	}
	return res
}

// mysqlSchemaDumpSections are the schema sections MySQL produces per database, in
// restore order (pre-data DDL before post-data triggers/routines/events).
var mysqlSchemaDumpSections = []core.DumpSection{
	core.DumpSectionPreData,
	core.DumpSectionPostData,
}

// distinctSchemas returns the distinct schema (database) names across the given
// table objects, preserving first-seen order for deterministic output.
func distinctSchemas(objects []core.Object) ([]string, error) {
	seen := make(map[string]struct{})
	var schemas []string
	for _, obj := range objects {
		table, err := payloadToTableDefinition(obj)
		if err != nil {
			return nil, fmt.Errorf("get table definition: %w", err)
		}
		if _, ok := seen[table.Schema]; ok {
			continue
		}
		seen[table.Schema] = struct{}{}
		schemas = append(schemas, table.Schema)
	}
	return schemas, nil
}

// schemaDumpDatabases returns the distinct MySQL databases (schemas) that own at
// least one allowed table, preserving first-seen order for deterministic output.
func schemaDumpDatabases(in core.ExplicitDumpContextInput) ([]string, error) {
	return distinctSchemas(allowedTableObjects(in))
}

// logSkippedSchemaDumps emits a debug log for every database present in the
// introspection that has no tables allowed by the filter, so its schema dump is
// skipped entirely.
func logSkippedSchemaDumps(ctx context.Context, in core.ExplicitDumpContextInput, inScopeDatabases []string) {
	allDatabases, err := distinctSchemas(in.IntrospectionResult.KindsMap[core.ObjectKindMysqlTable])
	if err != nil {
		// Best-effort logging only; payloads were already validated upstream.
		return
	}
	inScope := make(map[string]struct{}, len(inScopeDatabases))
	for _, d := range inScopeDatabases {
		inScope[d] = struct{}{}
	}
	for _, d := range allDatabases {
		if _, ok := inScope[d]; !ok {
			log.Ctx(ctx).Debug().
				Str("Database", d).
				Msg("skipping schema dump for database: no tables allowed by filter")
		}
	}
}

// buildSchemaDumpSpecs produces, for every in-scope database, a pre-data and a
// post-data schema dump spec. Specs are grouped by section (all pre-data first,
// then all post-data) so restore ordering is preserved. All MySQL schema dumps
// are delegated to a single mysqldump-backed dumper, so every spec carries the
// engine-level kind; the database is in Name and the section in Section.
func (b *ExplicitDumpContextBuilder) buildSchemaDumpSpecs(
	ctx context.Context,
	in core.ExplicitDumpContextInput,
	seq *core.TaskIDSequence,
) ([]core.SchemaDumpSpec, error) {
	databases, err := schemaDumpDatabases(in)
	if err != nil {
		return nil, fmt.Errorf("collect schema dump databases: %w", err)
	}
	logSkippedSchemaDumps(ctx, in, databases)

	databaseIDs := databaseObjectIDs(in)

	compression := mysqldumpOutputOptions(in.Config)

	var specs []core.SchemaDumpSpec
	for _, section := range mysqlSchemaDumpSections {
		for _, database := range databases {
			specs = append(specs, core.SchemaDumpSpec{
				TaskID:   seq.Next(),
				Kind:     core.SchemaObjectKindMysqlDatabase,
				ObjectID: databaseIDs[database],
				Payload: schemadump.Payload{
					Name:        database,
					Section:     section,
					Compression: compression,
				},
			})
		}
	}
	return specs, nil
}

// mysqldumpOutputOptions derives the schema-dump output options (compression)
// from the run configuration. The connection attributes (environment, flags,
// vendor options) are no longer resolved here: they are injected into the schema
// dumper at execution time via the ConnectionConfigurer. cfg is the pipeline's
// config.Config (passed through ExplicitDumpContextInput.Config); a nil/absent
// config yields defaults (used by unit tests that exercise spec shape without
// execution).
func mysqldumpOutputOptions(cfg any) core.Compression {
	c, ok := cfg.(*config.Config)
	if !ok || c == nil {
		return core.CompressionNone
	}
	return c.Dump.Options.Compression
}

// databaseObjectIDs maps each introspected database name to its runtime
// ObjectID, so a schema dump spec can reference the database object it targets.
func databaseObjectIDs(in core.ExplicitDumpContextInput) map[string]core.ObjectID {
	databaseObjects := in.IntrospectionResult.KindsMap[core.ObjectKindMysqlDatabase]
	ids := make(map[string]core.ObjectID, len(databaseObjects))
	for _, obj := range databaseObjects {
		ids[obj.Name] = obj.ID
	}
	return ids
}
