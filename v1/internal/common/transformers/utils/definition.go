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

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// NewTransformerFunc - make new transformer. This function receives Driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the Name. All those parameters has been defined in the TransformerDefinition object of the transformer
type NewTransformerFunc func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (
	Transformer, models.ValidationWarnings, error,
)

type TransformerDefinition struct {
	Properties      *TransformerProperties         `json:"properties"`
	New             NewTransformerFunc             `json:"-"`
	Parameters      []*toolkit.ParameterDefinition `json:"parameters"`
	SchemaValidator SchemaValidationFunc           `json:"-"`
}

func NewTransformerDefinition(
	properties *TransformerProperties, newTransformerFunc NewTransformerFunc,
	parameters ...*toolkit.ParameterDefinition,
) *TransformerDefinition {
	return &TransformerDefinition{
		Properties:      properties,
		New:             newTransformerFunc,
		Parameters:      parameters,
		SchemaValidator: DefaultSchemaValidator,
	}
}

func (d *TransformerDefinition) SetSchemaValidator(v SchemaValidationFunc) *TransformerDefinition {
	d.SchemaValidator = v
	return d
}

func (d *TransformerDefinition) Instance(
	ctx context.Context, driver *toolkit.Driver, rawParams map[string]toolkit.ParamsValue, dynamicParameters map[string]*toolkit.DynamicParamValue,
	whenCond string,
) (*TransformerContext, models.ValidationWarnings, error) {
	// DecodeValue parameters and get the pgcopy of parsed
	params, parametersWarnings, err := toolkit.InitParameters(driver, d.Parameters, rawParams, dynamicParameters)
	if err != nil {
		return nil, nil, err
	}

	if parametersWarnings.IsFatal() {
		return nil, parametersWarnings, nil
	}

	dynamicParams := make(map[string]*toolkit.DynamicParameter)
	staticParams := make(map[string]*toolkit.StaticParameter)
	for name, p := range params {
		switch v := p.(type) {
		case *toolkit.StaticParameter:
			staticParams[name] = v
		case *toolkit.DynamicParameter:
			dynamicParams[name] = v
		}
	}

	paramDefs := make(map[string]*toolkit.ParameterDefinition, len(d.Parameters))
	for _, pd := range d.Parameters {
		paramDefs[pd.Name] = pd
	}
	// Validate schema
	schemaWarnings, err := d.SchemaValidator(ctx, driver, d.Properties, staticParams)
	if err != nil {
		return nil, nil, fmt.Errorf("schema validation error: %w", err)
	}

	// Create a new transformer and receive warnings
	t, transformerWarnings, err := d.New(ctx, driver, params)
	if err != nil {
		return nil, nil, err
	}

	res := make(models.ValidationWarnings, 0, len(parametersWarnings)+len(schemaWarnings)+len(transformerWarnings))
	res = append(res, parametersWarnings...)
	res = append(res, schemaWarnings...)
	res = append(res, transformerWarnings...)

	meta := map[string]interface{}{
		"TableSchema": driver.Table.Schema,
		"TableName":   driver.Table.Name,
		"Transformer": d.Properties.Name,
	}

	when, condWarns := toolkit.NewWhenCond(whenCond, driver, meta)
	res = append(res, condWarns...)

	return &TransformerContext{
		Transformer:       t,
		StaticParameters:  staticParams,
		DynamicParameters: dynamicParams,
		When:              when,
	}, res, nil
}

type TransformerContext struct {
	Transformer       Transformer
	StaticParameters  map[string]*toolkit.StaticParameter
	DynamicParameters map[string]*toolkit.DynamicParameter
	When              *toolkit.WhenCond
}

func (tc *TransformerContext) EvaluateWhen(r *toolkit.Record) (bool, error) {
	return tc.When.Evaluate(r)
}
