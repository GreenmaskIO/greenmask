package dumpcontext

import (
	"context"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/conditions"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

// NewTableDriverFunc - function that uses to create a table driver for a specific DBMS driver.
// The column type override can be used in order to override driver encode-decode behaviour.
type NewTableDriverFunc func(
	vc *validationcollector.Collector,
	table commonmodels.Table,
	columnsTypeOverride map[string]string,
) (commonininterfaces.TableDriver, error)

// TableContextBuilder - produces list of TableContext that will be used in the task producer.
type TableContextBuilder struct {
	tables              []commonmodels.Table
	dumpQueries         []string
	tableConfigs        []commonmodels.TableConfig
	newTableDriver      NewTableDriverFunc
	transformerRegistry *transformerutils.TransformerRegistry
}

func New(
	tables []commonmodels.Table,
	dumpQueries []string,
	tableConfigs []commonmodels.TableConfig,
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

// Build - returns list of TableContext objects that are used in the TaskProducer interface.
func (p *TableContextBuilder) Build(ctx context.Context, vc *validationcollector.Collector) ([]TableContext, error) {
	var err error
	tableRuntimes := make([]TableContext, len(p.tables))
	for i := range p.tables {
		var transformationConfig commonmodels.TableConfig
		idx := slices.IndexFunc(p.tableConfigs, func(config commonmodels.TableConfig) bool {
			return p.tables[i].Schema == config.Schema && p.tables[i].Name == config.Name
		})
		if idx != -1 {
			transformationConfig = p.tableConfigs[idx]
		}
		query := p.dumpQueries[i]
		tableRuntimes[i], err = p.initTable(ctx, vc, p.tables[i], transformationConfig, query)
		if err != nil {
			return nil, fmt.Errorf("init table %s.%s: %w", p.tables[i].Schema, p.tables[i].Name, err)
		}
	}
	return tableRuntimes, nil
}

// initTable - initialize a table runtime for a specific table.
func (p *TableContextBuilder) initTable(
	ctx context.Context,
	vc *validationcollector.Collector,
	table commonmodels.Table,
	tableConfig commonmodels.TableConfig,
	dumpQueries string,
) (TableContext, error) {
	driver, err := p.newTableDriver(vc, table, tableConfig.ColumnsTypeOverride)
	if err != nil {
		return TableContext{}, fmt.Errorf("new driver: %w", err)
	}
	if dumpQueries == "" && tableConfig.Query != "" {
		dumpQueries = tableConfig.Query
	}
	tableCondition, err := p.compileTableCondition(vc, utils.Value(driver.Table()), tableConfig)
	if err != nil {
		return TableContext{}, fmt.Errorf("compile table condition: %w", err)
	}
	transformationRuntimes, err := p.initTableTransformers(ctx, vc, driver, tableConfig.Transformers)
	if err != nil {
		return TableContext{}, fmt.Errorf("init transformation runtimes: %w", err)
	}
	return TableContext{
		Table:              &table,
		TableCondition:     tableCondition,
		TransformerContext: transformationRuntimes,
		Query:              dumpQueries,
		TableDriver:        driver,
	}, nil
}

func (p *TableContextBuilder) initTableTransformers(
	ctx context.Context,
	vc *validationcollector.Collector,
	driver commonininterfaces.TableDriver,
	transformerConfigs []commonmodels.TransformerConfig,
) ([]*TransformerContext, error) {
	res := make([]*TransformerContext, len(transformerConfigs))
	for i := range transformerConfigs {
		transformer, err := p.initTransformer(ctx, vc, driver, transformerConfigs[i])
		if err != nil {
			return nil, fmt.Errorf("init transformer \"%s\": %w", transformerConfigs[i].Name, err)
		}
		transformerCond, err := p.compileTransformerCondition(vc, utils.Value(driver.Table()), transformerConfigs[i])
		if err != nil {
			return nil, fmt.Errorf("compile transformer condition: %w", err)
		}
		res[i] = &TransformerContext{
			Transformer: transformer,
			WhenCond:    transformerCond,
		}
	}
	return res, nil
}

func (p *TableContextBuilder) initTransformer(
	ctx context.Context,
	vc *validationcollector.Collector,
	driver commonininterfaces.TableDriver,
	config commonmodels.TransformerConfig,
) (commonininterfaces.Transformer, error) {
	vc = vc.WithMeta(map[string]any{"TransformerName": config.Name})
	transformerDefinition, ok := p.transformerRegistry.Get(config.Name)
	if !ok {
		vc.Add(commonmodels.NewValidationWarning().
			SetSeverity(commonmodels.ValidationSeverityError).
			SetMsg("transformer is not found"))
		return nil, fmt.Errorf("get transformer from registry: %w", commonmodels.ErrFatalValidationError)
	}
	params, err := parameters.InitParameters(
		vc,
		driver,
		transformerDefinition.Parameters,
		config.StaticParams,
		config.DynamicParams,
	)
	if err != nil {
		return nil, err
	}

	dynamicParams := make(map[string]*commonparameters.DynamicParameter)
	staticParams := make(map[string]*commonparameters.StaticParameter)
	for name, pp := range params {
		switch v := pp.(type) {
		case *commonparameters.StaticParameter:
			staticParams[name] = v
		case *commonparameters.DynamicParameter:
			dynamicParams[name] = v
		}
	}

	// Validate schema
	err = transformerDefinition.SchemaValidator(
		vc,
		utils.Value(driver.Table()),
		transformerDefinition.Properties,
		staticParams,
	)
	if err != nil {
		return nil, fmt.Errorf("schema validation error: %w", err)
	}

	// Create a new transformer
	return transformerDefinition.New(ctx, vc, driver, params)
}

func (p *TableContextBuilder) compileTransformerCondition(
	vc *validationcollector.Collector,
	table commonmodels.Table,
	transformerConfig commonmodels.TransformerConfig,
) (*conditions.WhenCond, error) {
	if transformerConfig.When == "" {
		return nil, nil
	}
	return conditions.NewWhenCond(vc, transformerConfig.When, table)
}

func (p *TableContextBuilder) compileTableCondition(
	vc *validationcollector.Collector,
	table commonmodels.Table,
	tableConfig commonmodels.TableConfig,
) (*conditions.WhenCond, error) {
	if tableConfig.When == "" {
		return nil, nil
	}
	return conditions.NewWhenCond(vc, tableConfig.When, table)
}
