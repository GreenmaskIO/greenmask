package transformers

import (
	"context"
	"fmt"

	"gopkg.in/yaml.v3"
)

type TransformationType string

const (
	AttributeTransformation TransformationType = "attribute"
	TupleTransformation     TransformationType = "tuple"
)

type ParamsValue []byte

func (pv ParamsValue) MarshalYAML() (interface{}, error) {
	var res = map[string]interface{}{}
	err := yaml.Unmarshal(pv, res)
	if err != nil {
		// fallback unmarshalling to string
		return string(pv), nil
	}

	return res, nil
}

// NewTransformerFunc - make new transformer. This function receives Driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the name. All those parameters has been defined in the Definition object of the transformer
type NewTransformerFunc func(ctx context.Context, driver *Driver, parameters map[string]*Parameter) (
	Transformer, ValidationWarnings, error,
)

type Definition struct {
	Properties      *TransformerProperties `json:"properties"`
	New             NewTransformerFunc     `json:"-"`
	Parameters      []*Parameter           `json:"parameters"`
	SchemaValidator SchemaValidationFunc   `json:"-"`
}

func NewDefinition(
	properties *TransformerProperties, newTransformerFunc NewTransformerFunc,
	parameters ...*Parameter,
) *Definition {
	return &Definition{
		Properties:      properties,
		New:             newTransformerFunc,
		Parameters:      parameters,
		SchemaValidator: DefaultSchemaValidator,
	}
}

func (d *Definition) SetSchemaValidator(v SchemaValidationFunc) *Definition {
	d.SchemaValidator = v
	return d
}

func (d *Definition) parseParameters(
	driver *Driver, rawParams map[string]ParamsValue, types []*Type,
) (ValidationWarnings, map[string]*Parameter, error) {
	if rawParams == nil && len(d.Parameters) > 0 {
		return ValidationWarnings{
			NewValidationWarning().
				SetMsg("parameters are required: received empty").
				SetSeverity("error"),
		}, nil, nil
	}

	var params = make(map[string]*Parameter, len(d.Parameters))
	for _, p := range d.Parameters {
		params[p.Name] = p.Copy()
	}
	var columnParameters = make(map[string]*Parameter)
	var commonParameters = make(map[string]*Parameter)
	for _, p := range d.Parameters {
		if p.IsColumn {
			columnParameters[p.Name] = p
		} else {
			commonParameters[p.Name] = p
		}
	}

	var totalWarnings ValidationWarnings
	// Column parameters parsing
	var columnParamsToSkip = make(map[string]struct{})
	for _, p := range columnParameters {
		warnings, err := p.Parse(driver, rawParams, nil, types)
		if err != nil {
			return nil, nil, fmt.Errorf("parameter %s parsing error: %w", p.Name, err)
		}
		if len(warnings) > 0 {
			totalWarnings = append(totalWarnings, warnings...)
			columnParamsToSkip[p.Name] = struct{}{}
		}
	}
	// Common parameters parsing
	for _, p := range commonParameters {
		if _, ok := columnParamsToSkip[p.LinkParameter]; p.LinkParameter != "" && ok {
			totalWarnings = append(totalWarnings, NewValidationWarning().
				AddMeta("ParameterName", p.Name).
				SetSeverity(WarningValidationSeverity).
				SetMsg("parameter skipping due to the error in the related parameter parsing"))
			continue
		}
		warnings, err := p.Parse(driver, rawParams, columnParameters, types)
		if err != nil {
			return nil, nil, fmt.Errorf("parameter %s parsing error: %w", p.Name, err)
		}
		if len(warnings) > 0 {
			totalWarnings = append(totalWarnings, warnings...)
		}
	}
	return totalWarnings, params, nil
}

func (d *Definition) Instance(
	ctx context.Context, driver *Driver, rawParams map[string]ParamsValue, types []*Type,
) (Transformer, ValidationWarnings, error) {
	// Parse parameters and get the pgcopy of parsed
	parametersWarnings, params, err := d.parseParameters(driver, rawParams, types)
	if err != nil {
		return nil, nil, err
	}

	if parametersWarnings.IsFatal() {
		return nil, parametersWarnings, nil
	}

	// Validate schema
	schemaWarnings, err := d.SchemaValidator(ctx, driver.Table, d.Properties, d.Parameters, types)
	if err != nil {
		return nil, nil, fmt.Errorf("schema validation error: %w", err)
	}

	// Create new transformer and receive warnings
	t, transformerWarnings, err := d.New(ctx, driver, params)
	if err != nil {
		return nil, nil, err
	}

	res := make(ValidationWarnings, 0, len(parametersWarnings)+len(schemaWarnings)+len(transformerWarnings))
	res = append(res, parametersWarnings...)
	res = append(res, schemaWarnings...)
	res = append(res, transformerWarnings...)

	return t, res, nil
}

func validateTransformation(transformationType TransformationType) error {
	if transformationType != AttributeTransformation && transformationType != TupleTransformation {
		return fmt.Errorf("unknown transformation type %s", transformationType)
	}
	return nil
}
