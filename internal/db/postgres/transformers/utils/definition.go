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
)

// NewTransformerFunc - make new transformer. This function receives Driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the Name. All those parameters has been defined in the TransformerDefinition object of the transformer
type NewTransformerFunc func(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.ParameterDefinition) (
	Transformer, toolkit.ValidationWarnings, error,
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

//func (d *TransformerDefinition) parseParameters(
//	Driver *toolkit.Driver, rawParams map[string]toolkit.ParamsValue, types []*toolkit.Type,
//) (toolkit.ValidationWarnings, map[string]*toolkit.ParameterDefinition, error) {
//	if rawParams == nil && len(d.Parameters) > 0 {
//		return toolkit.ValidationWarnings{
//			toolkit.NewValidationWarning().
//				SetMsg("parameters are required: received empty").
//				SetSeverity("error"),
//		}, nil, nil
//	}
//
//	var params = make(map[string]*toolkit.ParameterDefinition, len(d.Parameters))
//	for _, p := range d.Parameters {
//		params[p.Name] = p.Copy()
//	}
//	var columnParameters = make(map[string]*toolkit.ParameterDefinition)
//	var commonParameters = make(map[string]*toolkit.ParameterDefinition)
//	for _, p := range d.Parameters {
//		if p.IsColumn {
//			columnParameters[p.Name] = p
//		} else {
//			commonParameters[p.Name] = p
//		}
//	}
//
//	var totalWarnings toolkit.ValidationWarnings
//	// Column parameters parsing
//	var columnParamsToSkip = make(map[string]struct{})
//	for _, p := range columnParameters {
//		warnings, err := p.Decode(Driver, rawParams, nil, types)
//		if err != nil {
//			return nil, nil, fmt.Errorf("parameter %s parsing error: %w", p.Name, err)
//		}
//		if len(warnings) > 0 {
//			totalWarnings = append(totalWarnings, warnings...)
//			columnParamsToSkip[p.Name] = struct{}{}
//		}
//	}
//	// Common parameters parsing
//	for _, p := range commonParameters {
//		if _, ok := columnParamsToSkip[p.LinkColumnParameter]; p.LinkColumnParameter != "" && ok {
//			totalWarnings = append(totalWarnings, toolkit.NewValidationWarning().
//				AddMeta("ParameterName", p.Name).
//				SetSeverity(toolkit.WarningValidationSeverity).
//				SetMsg("parameter skipping due to the error in the related parameter parsing"))
//			continue
//		}
//		warnings, err := p.Decode(Driver, rawParams, columnParameters, types)
//		if err != nil {
//			return nil, nil, fmt.Errorf("parameter %s parsing error: %w", p.Name, err)
//		}
//		if len(warnings) > 0 {
//			totalWarnings = append(totalWarnings, warnings...)
//		}
//	}
//	return totalWarnings, params, nil
//}

func (d *TransformerDefinition) Instance(
	ctx context.Context, driver *toolkit.Driver, rawParams map[string]toolkit.ParamsValue, types []*toolkit.Type,
) (Transformer, toolkit.ValidationWarnings, error) {
	// Decode parameters and get the pgcopy of parsed
	params, parametersWarnings, err := toolkit.InitParameters(driver, rawParams, d.Parameters, types)
	if err != nil {
		return nil, nil, err
	}

	if parametersWarnings.IsFatal() {
		return nil, parametersWarnings, nil
	}

	// Validate schema
	schemaWarnings, err := d.SchemaValidator(ctx, driver.Table, d.Properties, params, types)
	if err != nil {
		return nil, nil, fmt.Errorf("schema validation error: %w", err)
	}

	// Create a new transformer and receive warnings
	t, transformerWarnings, err := d.New(ctx, driver, params)
	if err != nil {
		return nil, nil, err
	}

	res := make(toolkit.ValidationWarnings, 0, len(parametersWarnings)+len(schemaWarnings)+len(transformerWarnings))
	res = append(res, parametersWarnings...)
	res = append(res, schemaWarnings...)
	res = append(res, transformerWarnings...)

	return t, res, nil
}
