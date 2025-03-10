package parameters

import (
	"fmt"
	"maps"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func InitParameters(
	driver driver,
	paramDef []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) (map[string]Parameterizer, models.ValidationWarnings, error) {

	var warnings models.ValidationWarnings
	warnings = append(warnings, validateRequiredParameters(paramDef, staticValues, dynamicValues)...)
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	warnings = append(warnings, validateUnknownParameters(paramDef, staticValues, dynamicValues)...)
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	warnings = append(warnings, validateProvidedDynamicParametersSupportDynamicMode(paramDef, dynamicValues)...)
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	columnParamsDef, otherParamsDef := splitColumnsAndCommonParameters(paramDef)

	columnParams, columParamWarns, err := initColumnParameters(driver, columnParamsDef, staticValues)
	if err != nil {
		return nil, warnings, fmt.Errorf("initialize column parameters: %w", err)
	}
	warnings = append(warnings, columParamWarns...)
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	otherParams, otherParamsWarns, err := initOtherParameters(driver, otherParamsDef, staticValues, dynamicValues)
	if err != nil {
		return nil, warnings, fmt.Errorf("initialize non-column parameters: %w", err)
	}
	warnings = append(warnings, otherParamsWarns...)
	if warnings.IsFatal() {
		return nil, warnings, nil
	}

	params := make(map[string]Parameterizer, len(columnParams)+len(otherParams))
	maps.Copy(params, columnParams)
	maps.Copy(params, otherParams)

	return params, warnings, nil
}

func validateUnknownParameters(
	defs []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) models.ValidationWarnings {
	var warnings models.ValidationWarnings
	for name := range staticValues {
		if !slices.ContainsFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			warnings = append(
				warnings,
				models.NewValidationWarning().
					SetSeverity(models.ErrorValidationSeverity).
					SetMsg("received unknown parameter").
					AddMeta("ParameterName", name),
			)
		}

		// Check that value is static and dynamic value did not receive together
		if _, ok := dynamicValues[name]; ok {
			warnings = append(
				warnings,
				models.NewValidationWarning().
					SetSeverity(models.ErrorValidationSeverity).
					SetMsg("parameter value must be only static or dynamic at the same time").
					AddMeta("ParameterName", name),
			)
		}
	}

	for name := range dynamicValues {
		if !slices.ContainsFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			warnings = append(
				warnings,
				models.NewValidationWarning().
					SetSeverity(models.ErrorValidationSeverity).
					SetMsg("received unknown parameter").
					AddMeta("ParameterName", name),
			)
		}
		if _, ok := staticValues[name]; ok {
			warnings = append(
				warnings,
				models.NewValidationWarning().
					SetSeverity(models.ErrorValidationSeverity).
					SetMsg("parameter value must be only static or dynamic at the same time").
					AddMeta("ParameterName", name),
			)
		}
	}
	return warnings
}

func validateRequiredParameters(
	defs []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) models.ValidationWarnings {
	var requiredParamsWarns models.ValidationWarnings
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
		requiredParamsWarns = append(
			requiredParamsWarns,
			models.NewValidationWarning().
				SetMsg("parameter is required").
				AddMeta("ParameterName", pd.Name).
				SetSeverity(models.ErrorValidationSeverity),
		)
	}

	return requiredParamsWarns
}

func validateProvidedDynamicParametersSupportDynamicMode(
	defs []*ParameterDefinition,
	dynamicValues map[string]models.DynamicParamValue,
) models.ValidationWarnings {
	var warnings models.ValidationWarnings
	for name := range dynamicValues {
		idx := slices.IndexFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		})
		if idx == -1 {
			warnings = append(
				warnings,
				models.NewValidationWarning().
					SetSeverity(models.ErrorValidationSeverity).
					SetMsg("received unknown parameter").
					AddMeta("ParameterName", name),
			)
		}
		if defs[idx].DynamicModeProperties == nil {
			warnings = append(
				warnings,
				models.NewValidationWarning().
					SetSeverity(models.ErrorValidationSeverity).
					SetMsg("parameter does not support dynamic mode").
					AddMeta("ParameterName", name),
			)
		}
	}
	return warnings

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
	driver driver,
	columnParamsDef []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
) (map[string]Parameterizer, models.ValidationWarnings, error) {
	// Initialize column parameters
	var warnings models.ValidationWarnings
	params := make(map[string]Parameterizer)
	for _, pd := range columnParamsDef {
		// try to get the static value
		value, ok := staticValues[pd.Name]
		if !ok {
			return nil, warnings, fmt.Errorf("column parameter \"%s\" is not provided", pd.Name)
		}
		// TODO: Enrich parameters with ParameterName in Meta
		sp := NewStaticParameter(pd, driver)
		initWarns, err := sp.Init(nil, value)
		if err != nil {
			return nil, warnings, fmt.Errorf("error initializing \"%s\" parameter: %w", pd.Name, err)
		}
		for _, w := range initWarns {
			w.AddMeta("ParameterName", pd.Name)
		}
		warnings = append(warnings, initWarns...)
		params[pd.Name] = sp

	}
	return params, warnings, nil
}

func initOtherParameters(
	driver driver,
	otherParamsDef []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) (map[string]Parameterizer, models.ValidationWarnings, error) {
	var warnings models.ValidationWarnings
	params := make(map[string]Parameterizer)
	for _, pd := range otherParamsDef {
		dynamicValue, ok := dynamicValues[pd.Name]
		if ok {
			dp := NewDynamicParameter(pd, driver)
			initWarns, err := dp.Init(nil, dynamicValue)
			for _, w := range initWarns {
				w.AddMeta("ParameterName", pd.Name)
			}
			warnings = append(warnings, initWarns...)
			if err != nil {
				return nil, warnings, fmt.Errorf("initialize dynamic parameter \"%s\": %w", pd.Name, err)
			}
			params[pd.Name] = dp
			continue
		}

		staticValue := staticValues[pd.Name]
		sp := NewStaticParameter(pd, driver)
		initWarns, err := sp.Init(nil, staticValue)
		for _, w := range initWarns {
			w.AddMeta("ParameterName", pd.Name)
		}
		warnings = append(warnings, initWarns...)
		if err != nil {
			return nil, warnings, fmt.Errorf("initialize static parameter \"%s\": %w", pd.Name, err)
		}
		params[pd.Name] = sp
	}
	return params, warnings, nil
}
