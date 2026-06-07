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

package context

import (
	"context"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/common/conditions"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

// NewTableDriverFunc - function that uses to create a table driver for a specific DBMS driver.
// The column type override can be used in order to override driver encode-decode behaviour.
type NewTableDriverFunc func(
	ctx context.Context,
	table core.Table,
	columnsTypeOverride map[string]string,
) (core.TableDriver, error)

// TableContextBuilder - produces list of TableDumpContextPayload that will be used in the task producer.
type TableContextBuilder struct {
	tables              []core.Table
	dumpQueries         []string
	tableConfigs        []core.TableConfig
	newTableDriver      NewTableDriverFunc
	transformerRegistry *transformerutils.TransformerRegistry
}

func New(
	tables []core.Table,
	dumpQueries []string,
	tableConfigs []core.TableConfig,
	newDriverFunc NewTableDriverFunc,
	transformerRegistry *transformerutils.TransformerRegistry,
) *TableContextBuilder {
	return &TableContextBuilder{
		tables:              tables,
		dumpQueries:         dumpQueries,
		tableConfigs:        tableConfigs,
		newTableDriver:      newDriverFunc,
		transformerRegistry: transformerRegistry,
	}
}

// Build - returns list of TableDumpContextPayload objects that are used in the TaskProducer interface.
func (p *TableContextBuilder) Build(ctx context.Context) ([]TableDumpContextPayload, error) {
	var err error
	tableRuntimes := make([]TableDumpContextPayload, len(p.tables))
	ctx, err = utils.WithSaltFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("set salt: %w", err)
	}
	for i := range p.tables {
		var transformationConfig core.TableConfig
		idx := slices.IndexFunc(p.tableConfigs, func(config core.TableConfig) bool {
			return p.tables[i].Schema == config.Schema && p.tables[i].Name == config.Name
		})
		if idx != -1 {
			transformationConfig = p.tableConfigs[idx]
		}
		query := p.dumpQueries[i]
		tableRuntimes[i], err = p.initTable(ctx, p.tables[i], transformationConfig, query)
		if err != nil {
			return nil, fmt.Errorf("init table %s.%s: %w", p.tables[i].Schema, p.tables[i].Name, err)
		}
	}
	return tableRuntimes, nil
}

// initTable - initialize a table runtime for a specific table.
func (p *TableContextBuilder) initTable(
	ctx context.Context,
	table core.Table,
	tableConfig core.TableConfig,
	dumpQueries string,
) (TableDumpContextPayload, error) {
	ctx = log.Ctx(ctx).With().
		Str(core.MetaKeyTableSchema, table.Schema).
		Str(core.MetaKeyTableName, table.Name).
		Logger().WithContext(ctx)
	driver, err := p.newTableDriver(ctx, table, tableConfig.ColumnsTypeOverride)
	if err != nil {
		return TableDumpContextPayload{}, fmt.Errorf("new driver: %w", err)
	}
	if dumpQueries == "" && tableConfig.Query != "" {
		dumpQueries = tableConfig.Query
	}
	tableCondition, err := p.compileTableCondition(ctx, utils.Value(driver.Table()), tableConfig)
	if err != nil {
		return TableDumpContextPayload{}, fmt.Errorf("compile table condition: %w", err)
	}
	transformationRuntimes, err := p.initTableTransformers(ctx, driver, tableConfig.Transformers)
	if err != nil {
		return TableDumpContextPayload{}, fmt.Errorf("init transformation runtimes: %w", err)
	}
	return TableDumpContextPayload{
		Table:              &table,
		Condition:          tableCondition,
		TransformerContext: transformationRuntimes,
		Query:              dumpQueries,
		TableDriver:        driver,
	}, nil
}

func (p *TableContextBuilder) initTableTransformers(
	ctx context.Context,
	driver core.TableDriver,
	transformerConfigs []core.TransformerConfig,
) ([]*TransformerContext, error) {
	res := make([]*TransformerContext, len(transformerConfigs))
	for i := range transformerConfigs {
		ctx := log.Ctx(ctx).With().
			Str(core.MetaKeyTransformerName, transformerConfigs[i].Name).
			Logger().WithContext(ctx)
		initRes, err := p.initTransformer(ctx, driver, transformerConfigs[i])
		if err != nil {
			return nil, fmt.Errorf("init transformer \"%s\": %w", transformerConfigs[i].Name, err)
		}
		transformerCond, err := p.compileTransformerCondition(ctx, utils.Value(driver.Table()), transformerConfigs[i])
		if err != nil {
			return nil, fmt.Errorf("compile transformer condition: %w", err)
		}
		res[i] = &TransformerContext{
			Transformer:       initRes.transformer,
			Condition:         transformerCond,
			StaticParameters:  initRes.staticParameters,
			DynamicParameters: initRes.dynamicParameters,
		}
	}
	return res, nil
}

type tranInitRes struct {
	transformer       core.Transformer
	staticParameters  map[string]*parameters.DynamicParameter
	dynamicParameters map[string]*parameters.DynamicParameter
}

func (p *TableContextBuilder) initTransformer(
	ctx context.Context,
	driver core.TableDriver,
	config core.TransformerConfig,
) (tranInitRes, error) {
	ctx = validationcollector.WithMeta(ctx,
		core.MetaKeyTransformerName, config.Name,
	)
	transformerDefinition, ok := p.transformerRegistry.Get(config.Name)
	if !ok {
		validationcollector.FromContext(ctx).
			Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				SetMsg("transformer is not found"))
		return tranInitRes{}, fmt.Errorf("get transformer from registry: %w", core.ErrFatalValidationError)
	}
	params, err := parameters.InitParameters(
		ctx,
		driver,
		transformerDefinition.Parameters,
		config.StaticParams,
		config.DynamicParams,
		config.ResolveEnv,
	)
	if err != nil {
		return tranInitRes{}, err
	}

	dynamicParams := make(map[string]*parameters.DynamicParameter)
	staticParams := make(map[string]*parameters.StaticParameter)
	for name, pp := range params {
		switch v := pp.(type) {
		case *parameters.StaticParameter:
			staticParams[name] = v
		case *parameters.DynamicParameter:
			dynamicParams[name] = v
		}
	}

	// Validate schema
	err = transformerDefinition.SchemaValidator(
		ctx,
		utils.Value(driver.Table()),
		transformerDefinition.Properties,
		staticParams,
	)
	if err != nil {
		return tranInitRes{}, fmt.Errorf("schema validation error: %w", err)
	}

	// Create a new transformer
	tran, err := transformerDefinition.New(ctx, driver, params)
	if err != nil {
		return tranInitRes{}, fmt.Errorf("new transformer: %w", err)
	}
	return tranInitRes{
		transformer:       tran,
		dynamicParameters: dynamicParams,
		staticParameters:  dynamicParams,
	}, nil
}

func (p *TableContextBuilder) compileTransformerCondition(
	ctx context.Context,
	table core.Table,
	transformerConfig core.TransformerConfig,
) (core.CondEvaluator, error) {
	ctx = log.Ctx(ctx).With().
		Any(core.MetaKeyConditionScope, "Transformer").
		Logger().WithContext(ctx)
	if transformerConfig.When == "" {
		return nil, nil
	}
	return conditions.NewWhenCond(ctx, transformerConfig.When, table)
}

func (p *TableContextBuilder) compileTableCondition(
	ctx context.Context,
	table core.Table,
	tableConfig core.TableConfig,
) (core.CondEvaluator, error) {
	ctx = log.Ctx(ctx).With().
		Any(core.MetaKeyConditionScope, "Table").
		Logger().WithContext(ctx)
	if tableConfig.When == "" {
		return nil, nil
	}
	return conditions.NewWhenCond(ctx, tableConfig.When, table)
}
