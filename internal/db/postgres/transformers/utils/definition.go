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

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// NewTransformerFunc - make new transformer. This function receives Driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the Name. All those parameters has been defined in the TransformerDefinition object of the transformer
type NewTransformerFunc func(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (
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

type TransformerContext struct {
	Transformer       Transformer
	StaticParameters  map[string]*toolkit.StaticParameter
	DynamicParameters map[string]*toolkit.DynamicParameter
	whenCond          *vm.Program
	whenEnv           expr.Option
	rc                *toolkit.RecordContext
}

func (tc *TransformerContext) SetRecord(r *toolkit.Record) {
	tc.rc.SetRecord(r)
}

func (tc *TransformerContext) EvaluateWhen() (bool, error) {
	if tc.whenCond == nil {
		return true, nil
	}

	output, err := expr.Run(tc.whenCond, nil)
	if err != nil {
		return false, fmt.Errorf("unable to evaluate when condition: %w", err)
	}

	cond, ok := output.(bool)
	if ok {
		return cond, nil
	}

	return false, fmt.Errorf("when condition should return boolean, got (%T) and value %+v", cond, cond)
}

func (d *TransformerDefinition) Instance(
	ctx context.Context, driver *toolkit.Driver, rawParams map[string]toolkit.ParamsValue, dynamicParameters map[string]*toolkit.DynamicParamValue,
	whenCond string,
) (*TransformerContext, toolkit.ValidationWarnings, error) {
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

	res := make(toolkit.ValidationWarnings, 0, len(parametersWarnings)+len(schemaWarnings)+len(transformerWarnings))
	res = append(res, parametersWarnings...)
	res = append(res, schemaWarnings...)
	res = append(res, transformerWarnings...)

	cond, rc, condWarns := compileCond(whenCond, driver)
	res = append(res, condWarns...)

	return &TransformerContext{
		Transformer:       t,
		StaticParameters:  staticParams,
		DynamicParameters: dynamicParams,
		whenCond:          cond,
		rc:                rc,
	}, res, nil
}

func compileCond(whenCond string, driver *toolkit.Driver) (*vm.Program, *toolkit.RecordContext, toolkit.ValidationWarnings) {
	if whenCond == "" {
		return nil, nil, nil
	}
	rc, funcs := newRecordContext(driver)

	cond, err := expr.Compile(whenCond, funcs...)
	if err != nil {
		return nil, nil, toolkit.ValidationWarnings{
			toolkit.NewValidationWarning().
				SetSeverity(toolkit.ErrorValidationSeverity).
				AddMeta("Error", err.Error()).
				SetMsg("unable to compile when condition"),
		}
	}

	return cond, rc, nil
}

func newRecordContext(driver *toolkit.Driver) (*toolkit.RecordContext, []expr.Option) {
	var funcs []expr.Option
	rctx := toolkit.NewRecordContext()
	for _, c := range driver.Table.Columns {

		f := expr.Function(
			c.Name,
			func(name string) func(params ...any) (any, error) {
				return func(params ...any) (any, error) {
					return rctx.GetColumnRawValue(name)
				}
			}(c.Name),
		)
		funcs = append(funcs, f)
	}
	return rctx, funcs
}
