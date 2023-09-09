package greenmask_tmp

import (
	"errors"

	"github.com/greenmaskio/greenmask/internal/domains"
)

type ParametersDefinition map[string]ParameterMeta

type ParameterMeta struct {
	Type     string
	Default  string
	Required bool
	IsColumn bool
	PgTypes  []string
}

type TransformerSettings struct {
	Name               string   `json:"name,omitempty"`
	Description        string   `json:"description,omitempty"`
	Nullable           bool     `json:"nullable,omitempty"`
	Variadic           bool     `json:"variadic,omitempty"`
	Unique             bool     `json:"unique,omitempty"`
	MaxLength          int64    `json:"maxLength,omitempty"`
	SupportedTypes     []string `json:"supportedTypes,omitempty"`
	TransformationType string   `json:"transformationType,omitempty"`
}

type CustomTransformerMeta struct {
	Name       string               `json:"name,omitempty"`
	BinaryPath string               `json:"binaryPath,omitempty"`
	Parameters ParametersDefinition `json:"parameters,omitempty"`
	Validate   string               `json:"validate,omitempty"`
	Settings   TransformerSettings  `json:"settings,omitempty"`
}

func (ctm *CustomTransformerMeta) Instance() (domains.Transformer, error) {
	// Instancing path
	// 1. Check binary file existence
	// 2. Get meta if possible
	// 2.1 Setup TransformerSettings accordingly
	// 3. Return initialized transformer
	return nil, errors.New("unknown proto")
}
