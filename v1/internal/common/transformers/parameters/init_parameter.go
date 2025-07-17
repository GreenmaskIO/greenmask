package parameters

import (
	"fmt"
	"maps"
	"slices"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func InitParameters(
	vc *validationcollector.Collector,
	driver commonininterfaces.TableDriver,
	paramDef []*ParameterDefinition,
	staticValues map[string]commonmodels.ParamsValue,
	dynamicValues map[string]commonmodels.DynamicParamValue,
) (map[string]Parameterizer, error) {
	validateRequiredParameters(vc, paramDef, staticValues, dynamicValues)
	if vc.IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	validateUnknownParameters(vc, paramDef, staticValues, dynamicValues)
	if vc.IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	validateProvidedDynamicParametersSupportDynamicMode(vc, paramDef, dynamicValues)
	if vc.IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	columnParamsDef, otherParamsDef := splitColumnsAndCommonParameters(paramDef)

	columnParams, err := initColumnParameters(vc, driver, columnParamsDef, staticValues)
	if err != nil {
		return nil, fmt.Errorf("initialize column parameters: %w", err)
	}
	if vc.IsFatal() {
		return nil, commonmodels.ErrFatalValidationError
	}

	otherParams, err := initOtherParameters(vc, driver, otherParamsDef, staticValues, dynamicValues, columnParams)
	if err != nil {
		return nil, fmt.Errorf("initialize non-column parameters: %w", err)
	}
	if vc.IsFatal() {
		return nil, nil
	}

	params := make(map[string]Parameterizer, len(columnParams)+len(otherParams))
	maps.Copy(params, columnParams)
	maps.Copy(params, otherParams)

	return params, nil
}

func validateUnknownParameters(
	vc *validationcollector.Collector,
	defs []*ParameterDefinition,
	staticValues map[string]models.ParamsValue,
	dynamicValues map[string]models.DynamicParamValue,
) {
	for name := range staticValues {
		if !slices.ContainsFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received unknown parameter").
				AddMeta("ParameterName", name))
		}

		// Check that value is static and dynamic value did not receive together
		if _, ok := dynamicValues[name]; ok {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter value must be only static or dynamic at the same time").
				AddMeta("ParameterName", name))
		}
	}

	for name := range dynamicValues {
		if !slices.ContainsFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		}) {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received unknown parameter").
				AddMeta("ParameterName", name))
		}
		if _, ok := staticValues[name]; ok {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("parameter value must be only static or dynamic at the same time").
				AddMeta("ParameterName", name))
		}
	}
}

func validateRequiredParameters(
	vc *validationcollector.Collector,
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
		vc.Add(models.NewValidationWarning().
			SetMsg("parameter is required").
			AddMeta("ParameterName", pd.Name).
			SetSeverity(models.ValidationSeverityError))
	}
}

func validateProvidedDynamicParametersSupportDynamicMode(
	vc *validationcollector.Collector,
	defs []*ParameterDefinition,
	dynamicValues map[string]models.DynamicParamValue,
) {
	for name := range dynamicValues {
		idx := slices.IndexFunc(defs, func(definition *ParameterDefinition) bool {
			return definition.Name == name
		})
		if idx == -1 {
			vc.Add(models.NewValidationWarning().
				SetSeverity(models.ValidationSeverityError).
				SetMsg("received unknown parameter").
				AddMeta("ParameterName", name))
		}
		if defs[idx].DynamicModeProperties == nil {
			vc.Add(models.NewValidationWarning().
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
	vc *validationcollector.Collector,
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

		if err := sp.Init(
			vc.WithMeta(
				map[string]any{"ParameterName": pd.Name},
			),
			nil,
			value,
		); err != nil {
			return nil, fmt.Errorf("error initializing \"%s\" parameter: %w", pd.Name, err)
		}
		params[pd.Name] = sp

	}
	return params, nil
}

func initOtherParameters(
	vc *validationcollector.Collector,
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
		if ok {
			dp := NewDynamicParameter(pd, driver)
			if err := dp.Init(
				// Add meta to the validation collector so it will store the warnings with the proper parameter name
				// in the Meat.
				vc.WithMeta(
					map[string]any{"ParameterName": pd.Name},
				),
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
			vc.WithMeta(
				map[string]any{commonmodels.MetaKeyParameterName: pd.Name},
			),
			assertedColumnParameters,
			staticValue,
		); err != nil {
			return nil, fmt.Errorf("initialize static parameter \"%s\": %w", pd.Name, err)
		}
		params[pd.Name] = sp
	}
	return params, nil
}
