package base

import "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"

type TransformationType int

const (
	AttributeTransformation = iota
	TupleTransformation
	ShiftTransformation
	BlendTransformation
)

type NewTransformerFunc func(driver *transformers.Driver) (Transformer, error)

type Properties struct {
	Name               string             `json:"name"`
	Description        string             `json:"description"`
	Nullable           bool               `json:"nullable,omitempty"`
	Variadic           bool               `json:"variadic,omitempty"`
	Unique             bool               `json:"unique,omitempty"`
	MaxLength          int64              `json:"maxLength,omitempty"`
	TransformationType TransformationType `json:"transformationType,omitempty"`
	Extended           map[string]string  `json:"extended,omitempty"`
	//Validate           bool               `json:"validate,omitempty"`
	//IsCustom           bool               `json:"isCustom,omitempty"`
}

func NewProperties(name, description string) *Properties {
	return &Properties{
		Name:               name,
		Description:        description,
		MaxLength:          -1,
		TransformationType: TupleTransformation,
		Extended:           make(map[string]string),
	}
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

type Definition struct {
	Properties *Properties
	New        NewTransformerFunc
}
