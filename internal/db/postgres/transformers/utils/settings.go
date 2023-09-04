package utils

import "github.com/GreenmaskIO/greenmask/internal/domains"

type TransformerSettings struct {
	Name               string                     `json:"name,omitempty"`
	Description        string                     `json:"description,omitempty"`
	Nullable           bool                       `json:"nullable,omitempty"`
	Variadic           bool                       `json:"variadic,omitempty"`
	Unique             bool                       `json:"unique,omitempty"`
	MaxLength          int64                      `json:"maxLength,omitempty"`
	TransformationType domains.TransformationType `json:"transformationType,omitempty"`
	// Custom transformer settings
	Validate  bool `json:"validate,omitempty"`
	Proto     bool `json:"proto,omitempty"`
	Streaming bool `json:"streaming,omitempty"`

	// SupportedOids - list of the supported pg type oids. - will be replaced SupportedTypes instead
	// Deprecated
	SupportedOids  []int
	SupportedTypes []string
	CastVar        interface{}
	IsCustom       bool
}

func NewTransformerSettings() *TransformerSettings {
	return &TransformerSettings{
		TransformationType: domains.AttributeTransformation,
	}
}

func (tbs *TransformerSettings) SetVariadic() *TransformerSettings {
	tbs.Variadic = true
	return tbs
}

func (tbs *TransformerSettings) SetNullable() *TransformerSettings {
	tbs.Nullable = true
	return tbs
}

func (tbs *TransformerSettings) SetUnique() *TransformerSettings {
	tbs.Unique = true
	return tbs
}

func (tbs *TransformerSettings) SetMaxLength(length int64) *TransformerSettings {
	tbs.MaxLength = length
	return tbs
}

func (tbs *TransformerSettings) SetSupportedOids(oids ...int) *TransformerSettings {
	tbs.SupportedOids = oids
	return tbs
}

func (tbs *TransformerSettings) SetCastVar(castVar interface{}) *TransformerSettings {
	tbs.CastVar = castVar
	return tbs
}

func (tbs *TransformerSettings) SetTransformationType(tt domains.TransformationType) *TransformerSettings {
	tbs.TransformationType = tt
	return tbs
}

func (tbs *TransformerSettings) SetName(name string) *TransformerSettings {
	tbs.Name = name
	return tbs
}
