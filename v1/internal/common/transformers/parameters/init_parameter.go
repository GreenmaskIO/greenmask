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

package parameters

import (
	"context"
	"fmt"
	"maps"
	"slices"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func InitParameters(
	ctx context.Context,
	driver commonininterfaces.TableDriver,
	paramDef []*ParameterDefinition,
	staticValues map[string]commonmodels.ParamsValue,
	dynamicValues map[string]commonmodels.DynamicParamValue,
) (map[string]Parameterizer, error) {
	validateRequiredParameters(ctx, paramDef, staticValues, dynamicValues)
	if validationcollector.FromContext(ctx).IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	validateUnknownParameters(ctx, paramDef, staticValues, dynamicValues)
	if validationcollector.FromContext(ctx).IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	validateProvidedDynamicParametersSupportDynamicMode(ctx, paramDef, dynamicValues)
	if validationcollector.FromContext(ctx).IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	columnParamsDef, otherParamsDef := splitColumnsAndCommonParameters(paramDef)

	columnParams, err := initColumnParameters(ctx, driver, columnParamsDef, staticValues)
	if err != nil {
		return nil, fmt.Errorf("initialize column parameters: %w", err)
	}
	if validationcollector.FromContext(ctx).IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	otherParams, err := initOtherParameters(ctx, driver, otherParamsDef, staticValues, dynamicValues, columnParams)
	if err != nil {
		return nil, fmt.Errorf("initialize non-column parameters: %w", err)
	}
	if validationcollector.FromContext(ctx).IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	params := make(map[string]Parameterizer, len(columnParams)+len(otherParams))
	maps.Copy(params, columnParams)
	maps.Copy(params, otherParams)

	return params, nil
}

func validateUnknownParameters(
	ctx context.Context,
	defs []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) {
	for name := range staticValues {
		if !slices.ContainsFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received unknown parameter").
				AddMeta("ParameterName", name))
		}

		// Check that value is static and dynamic value did not receive together
		if _, ok := dynamicValues[name]; ok {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter value must be only static or dynamic at the same time").
				AddMeta("ParameterName", name))
		}
	}

	for name := range dynamicValues {
		if !slices.ContainsFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received unknown parameter").
				AddMeta("ParameterName", name))
		}
		if _, ok := staticValues[name]; ok {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter value must be only static or dynamic at the same time").
				AddMeta("ParameterName", name))
		}
	}
}

func validateRequiredParameters(
	ctx context.Context,
	defs []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) {
	// Check is there any parameters
	for _, pd := range defs {
		if !pd.Required {
			// Skip not required parameters
			continue
		}
		if _, ok := staticValues[pd.Name]; ok {
			// Static parameter is received
			continue
		}
		if _, ok := dynamicValues[pd.Name]; ok {
			// Dynamic parameter is received
			continue
		}

		// Required parameter is not received
		validationcollector.FromContext(ctx).
			Add(models.NewValidationWarning().
				SetMsg("parameter is required").
				AddMeta("ParameterName", pd.Name).
				SetSeverity(models.ValidationSeverityError))
	}
}

func validateProvidedDynamicParametersSupportDynamicMode(
	ctx context.Context,
	defs []*ParameterDefinition,
	dynamicValues map[string]models.DynamicParamValue,
) {
	for name := range dynamicValues {
		idx := slices.IndexFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		})
		if idx == -1 {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received unknown parameter").
				AddMeta("ParameterName", name))
		}
		if defs[idx].DynamicModeProperties == nil {
			validationcollector.FromContext(ctx).Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter does not support dynamic mode").
				AddMeta("ParameterName", name))
		}
	}
}

// splitColumnsAndCommonParameters separates column parameters from the others in the list of parameter definitions
// and returns two slices: the first one contains column parameters, the second one contains the others.
func splitColumnsAndCommonParameters(paramDef []*ParameterDefinition) (
	[]*ParameterDefinition, []*ParameterDefinition,
) {
	var columnParamsDef []*ParameterDefinition
	var otherParamsDef []*ParameterDefinition
	for _, pd := range paramDef {
		if pd.IsColumn {
			columnParamsDef = append(columnParamsDef, pd)
		} else {
			otherParamsDef = append(otherParamsDef, pd)
		}
	}
	return columnParamsDef, otherParamsDef
}

func initColumnParameters(
	ctx context.Context,
	driver commonininterfaces.TableDriver,
	columnParamsDef []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
) (map[string]Parameterizer, error) {
	// Initialize column parameters
	params := make(map[string]Parameterizer)
	for _, pd := range columnParamsDef {
		// try to get the static value
		value, ok := staticValues[pd.Name]
		if !ok {
			return nil, fmt.Errorf("column parameter \"%s\" is not provided", pd.Name)
		}
		sp := NewStaticParameter(pd, driver)

		ctx = validationcollector.WithCollector(ctx, validationcollector.FromContext(ctx).
			WithMeta(
				map[string]any{"ParameterName": pd.Name},
			),
		)
		if err := sp.Init(
			ctx,
			nil,
			value,
		); err != nil {
			return nil, fmt.Errorf("initialize \"%s\" parameter: %w", pd.Name, err)
		}
		params[pd.Name] = sp

	}
	return params, nil
}

func initOtherParameters(
	ctx context.Context,
	driver commonininterfaces.TableDriver,
	otherParamsDef []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
	columnParams map[string]Parameterizer,
) (map[string]Parameterizer, error) {
	params := make(map[string]Parameterizer)
	assertedColumnParameters := make(map[string]*StaticParameter, len(columnParams))
	for k, v := range columnParams {
		vv, ok := v.(*StaticParameter)
		if !ok {
			panic("invalid parameter type")
		}
		assertedColumnParameters[k] = vv
	}
	for _, pd := range otherParamsDef {
		dynamicValue, ok := dynamicValues[pd.Name]
		ctx = validationcollector.WithCollector(ctx, validationcollector.FromContext(ctx).
			WithMeta(
				map[string]any{"ParameterName": pd.Name},
			),
		)
		if ok {
			dp := NewDynamicParameter(pd, driver)
			// try to get the dynamic value
			if err := dp.Init(
				// Add meta to the validation collector so it will store the warnings with the proper parameter name
				// in the Meat.
				ctx,
				assertedColumnParameters,
				dynamicValue,
			); err != nil {
				return nil, fmt.Errorf("initialize dynamic parameter \"%s\": %w", pd.Name, err)
			}
			params[pd.Name] = dp
			continue
		}

		staticValue := staticValues[pd.Name]
		sp := NewStaticParameter(pd, driver)
		if err := sp.Init(
			ctx,
			assertedColumnParameters,
			staticValue,
		); err != nil {
			return nil, fmt.Errorf("initialize static parameter \"%s\": %w", pd.Name, err)
		}
		params[pd.Name] = sp
	}
	return params, nil
}
