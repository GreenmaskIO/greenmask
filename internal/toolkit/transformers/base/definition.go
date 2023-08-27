package base

import (
	"context"
	"fmt"
	"github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
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
type NewTransformerFunc func(ctx context.Context, driver *transformers.Driver, parameters map[string]*transformers.Parameter) (Transformer, error)

type Properties struct {
	Name               string                 `json:"name"`
	Description        string                 `json:"description"`
	Nullable           bool                   `json:"nullable,omitempty"`
	Variadic           bool                   `json:"variadic,omitempty"`
	Unique             bool                   `json:"unique,omitempty"`
	MaxLength          int64                  `json:"maxLength,omitempty"`
	TransformationType TransformationType     `json:"transformationType,omitempty"`
	Extended           map[string]interface{} `json:"extended,omitempty"`
	//Validate           bool               `json:"validate,omitempty"`
	//IsCustom           bool               `json:"isCustom,omitempty"`
}

func MustNewProperties(name, description string, transformationType TransformationType) *Properties {
	p, err := NewProperties(name, description, transformationType)
	if err != nil {
		panic(err.Error())
	}
	return p
}

func NewProperties(name, description string, transformationType TransformationType) (*Properties, error) {
	if err := validateTransformation(transformationType); err != nil {
		return nil, err
	}

	return &Properties{
		Name:               name,
		Description:        description,
		MaxLength:          -1,
		TransformationType: transformationType,
		Extended:           make(map[string]interface{}),
	}, nil
}

func (p *Properties) SetNullable() *Properties {
	p.Nullable = true
	return p
}

func (p *Properties) SetVariadic() *Properties {
	p.Variadic = true
	return p
}

func (p *Properties) SetUnique() *Properties {
	p.Unique = true
	return p
}

func (p *Properties) SetMaxLength(l int64) *Properties {
	p.MaxLength = l
	return p
}

func (p *Properties) SetTransformationType(transformationType TransformationType) *Properties {
	if err := validateTransformation(transformationType); err != nil {
		panic(err.Error())
	}
	p.TransformationType = transformationType
	return p
}

func (p *Properties) SetExtended(extended map[string]interface{}) *Properties {
	p.Extended = extended
	return nil
}

type Definition struct {
	Properties   *Properties
	New          NewTransformerFunc
	Parameters   []*transformers.Parameter
	parameterMap map[string]*transformers.Parameter
}

func (d *Definition) ParseParameters(driver *transformers.Driver, params map[string][]byte) error {
	d.parameterMap = make(map[string]*transformers.Parameter, len(d.Parameters))
	for _, p := range d.Parameters {
		if err := p.Parse(driver, params); err != nil {
			return fmt.Errorf("parameter parsing error: %w", err)
		}
		d.parameterMap[p.Name] = p
	}
	return nil
}

func validateTransformation(transformationType TransformationType) error {
	if transformationType != AttributeTransformation && transformationType != TupleTransformation &&
		transformationType != BlendTransformation {
		return fmt.Errorf("unknown transformation type %s", transformationType)
	}
	return nil
}
