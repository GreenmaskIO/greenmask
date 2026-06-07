// Copyright 2023 Greenmask
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

package utils

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/conditions"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	parameters2 "github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/rs/zerolog/log"
)

// NewTransformerFunc - make new transformer. This function receives Driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the ID. All those parameters has been defined in the TransformerDefinition object of the transformer
type NewTransformerFunc func(
	ctx context.Context,
	tableDriver core.TableDriver,
	parameters map[string]parameters2.Parameterizer,
) (core.Transformer, error)

type TransformerDefinition struct {
	Properties      *TransformerProperties             `json:"properties"`
	New             NewTransformerFunc                 `json:"-"`
	Parameters      []*parameters2.ParameterDefinition `json:"parameters"`
	SchemaValidator SchemaValidationFunc               `json:"-"`
}

func NewTransformerDefinition(
	properties *TransformerProperties, newTransformerFunc NewTransformerFunc,
	parameters ...*parameters2.ParameterDefinition,
) *TransformerDefinition {
	return &TransformerDefinition{
		Properties:      properties,
		New:             newTransformerFunc,
		Parameters:      parameters,
		SchemaValidator: DefaultSchemaValidator,
	}
}

func (d *TransformerDefinition) ValidateColumnParameters(
	ctx context.Context,
	table core.Table,
	columnParameters map[string]*parameters2.StaticParameter,
) error {
	if d.SchemaValidator == nil {
		return nil
	}
	return d.SchemaValidator(ctx, table, d.Properties, columnParameters)
}

func (d *TransformerDefinition) SetSchemaValidator(v SchemaValidationFunc) *TransformerDefinition {
	d.SchemaValidator = v
	return d
}

func (d *TransformerDefinition) Init(
	ctx context.Context,
	driver core.TableDriver,
	config core.TransformerConfig,
) (core.TransformerContexter, error) {
	ctx = validationcollector.WithMeta(ctx,
		core.MetaKeyTransformerName, config.Name,
	)
	params, err := parameters2.InitParameters(
		ctx,
		driver,
		d.Parameters,
		config.StaticParams,
		config.DynamicParams,
		config.ResolveEnv,
	)
	if err != nil {
		return nil, fmt.Errorf("init parameters: %w", err)
	}

	dynamicParams := make(map[string]*parameters2.DynamicParameter)
	staticParams := make(map[string]*parameters2.StaticParameter)
	for name, pp := range params {
		switch v := pp.(type) {
		case *parameters2.StaticParameter:
			staticParams[name] = v
		case *parameters2.DynamicParameter:
			dynamicParams[name] = v
		}
	}

	// Validate schema
	err = d.SchemaValidator(
		ctx,
		utils.Value(driver.Table()),
		d.Properties,
		staticParams,
	)
	if err != nil {
		return nil, fmt.Errorf("schema validation error: %w", err)
	}

	// Create a new transformer
	tran, err := d.New(ctx, driver, params)
	if err != nil {
		return nil, fmt.Errorf("new transformer: %w", err)
	}
	ctx = log.Ctx(ctx).With().
		Any(core.MetaKeyConditionScope, "Transformer").
		Logger().WithContext(ctx)

	var whenCond *conditions.WhenCond
	if config.When != "" {
		whenCond, err = conditions.NewWhenCond(ctx, config.When, utils.Value(driver.Table()))
		if err != nil {
			return nil, err
		}
	}

	return &transformercontext.TransformerContext{
		Transformer:       tran,
		Condition:         whenCond,
		StaticParameters:  staticParams,
		DynamicParameters: dynamicParams,
	}, nil
}
