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

// Package tablebuilder provides RDBMS-agnostic helpers for initialising table
// transformers and resolving per-table configuration during dump context building.
package tablebuilder

import (
	"context"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/common/conditions"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

// GetTableConfig returns the TableConfig for the given table, or nil if none is configured.
func GetTableConfig(tableConfigs []core.TableConfig, table core.Table) *core.TableConfig {
	idx := slices.IndexFunc(tableConfigs, func(cfg core.TableConfig) bool {
		return table.Schema == cfg.Schema && table.Name == cfg.Name
	})
	if idx != -1 {
		return &tableConfigs[idx]
	}
	return nil
}

// GetTableSubsetQuery returns the subset WHERE query for the given object, or empty string if none.
func GetTableSubsetQuery(subsetRes core.SubsetResult, obj core.Object) string {
	return subsetRes.SubsetMap[obj.ID]
}

// CompileTableCondition compiles the table-level when condition, returning nil when unset.
func CompileTableCondition(
	ctx context.Context,
	table core.Table,
	tableConfig *core.TableConfig,
) (core.CondEvaluator, error) {
	if tableConfig == nil || tableConfig.When == "" {
		return nil, nil
	}
	ctx = log.Ctx(ctx).With().
		Any(core.MetaKeyConditionScope, "Table").
		Logger().WithContext(ctx)
	return conditions.NewWhenCond(ctx, tableConfig.When, table)
}

// initTransformer looks up a transformer in the registry and initialises it via the provisioner.
func initTransformer(
	ctx context.Context,
	driver core.TableDriver,
	config core.TransformerConfig,
	registry core.TransformerRegistry,
) (core.TransformerContexter, error) {
	ctx = validationcollector.WithMeta(ctx, core.MetaKeyTransformerName, config.Name)
	provisioner, ok := registry.Get(config.Name)
	if !ok {
		validationcollector.FromContext(ctx).
			Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				SetMsg("transformer is not found"))
		return nil, fmt.Errorf("get transformer from registry: %w", core.ErrFatalValidationError)
	}
	transformerCtx, err := provisioner.Init(ctx, driver, config)
	if err != nil {
		return nil, fmt.Errorf("init transformer: %w", err)
	}
	return transformerCtx, nil
}

// InitTableTransformers initialises all transformer configs for a single table in order.
func InitTableTransformers(
	ctx context.Context,
	driver core.TableDriver,
	transformerConfigs []core.TransformerConfig,
	registry core.TransformerRegistry,
) ([]core.TransformerContexter, error) {
	res := make([]core.TransformerContexter, len(transformerConfigs))
	for i := range transformerConfigs {
		ctx := log.Ctx(ctx).With().
			Str(core.MetaKeyTransformerName, transformerConfigs[i].Name).
			Logger().WithContext(ctx)
		var err error
		res[i], err = initTransformer(ctx, driver, transformerConfigs[i], registry)
		if err != nil {
			return nil, fmt.Errorf("init transformer %q: %w", transformerConfigs[i].Name, err)
		}
	}
	return res, nil
}
