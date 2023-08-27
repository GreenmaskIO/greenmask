package transformers

import (
	"context"
	"fmt"
)

type TransformationType string

const (
	AttributeTransformation TransformationType = "attribute"
	TupleTransformation     TransformationType = "tuple"
	BlendTransformation     TransformationType = "blend"
)

// NewTransformerFunc - make new transformer. This function receives driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the name. All those parameters has been defined in the Definition object of the transformer
type NewTransformerFunc func(ctx context.Context, driver *Driver, parameters map[string]*Parameter) (Transformer, error)

type Definition struct {
	Properties      *Properties
	New             NewTransformerFunc
	Parameters      []*Parameter
	SchemaValidator SchemaValidationFunc
}

func NewDefinition(properties *Properties, newTransformerFunc NewTransformerFunc,
	parameters []*Parameter) *Definition {
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

func (d *Definition) parseParameters(driver *Driver, rawParams map[string][]byte) (map[string]*Parameter, error) {
	var params = make(map[string]*Parameter, len(d.Parameters))
	for _, p := range d.Parameters {
		params[p.Name] = &(*p)
	}
	for _, p := range params {
		if err := p.Parse(driver, rawParams); err != nil {
			return nil, fmt.Errorf("parameter %s parsing error: %w", p.Name, err)
		}
	}
	return params, nil
}

func (d *Definition) Instance(ctx context.Context, driver *Driver, rawParams map[string][]byte) (Transformer, ValidationWarnings, error) {
	// Parse parameters and get the copy of parsed
	params, err := d.parseParameters(driver, rawParams)
	if err != nil {
		return nil, nil, err
	}

	// Validate schema
	schemaWarnings, err := d.SchemaValidator(driver.Table, d.Properties, d.Parameters)
	if err != nil {
		return nil, nil, fmt.Errorf("schema validation error: %w", err)
	}

	// Create new transformer
	t, err := d.New(ctx, driver, params)
	if err != nil {
		return nil, nil, err
	}

	// Perform transformer validation
	transformerWarnings, err := t.Validate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("transformer validation error: %w", err)
	}

	return t, append(schemaWarnings, transformerWarnings...), nil
}

func validateTransformation(transformationType TransformationType) error {
	if transformationType != AttributeTransformation && transformationType != TupleTransformation &&
		transformationType != BlendTransformation {
		return fmt.Errorf("unknown transformation type %s", transformationType)
	}
	return nil
}
