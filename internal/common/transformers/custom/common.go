package custom

import (
	"errors"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	PostgresEngine = "PostgreSQL"
	MySqlEngine    = "MySQL"
	MongoDbEngine  = "MongoDb"
)

const (
	NoValidate      = "NoValidate"
	ValidateBuiltIn = "ValidateBuildIn"
	ValidateCustom  = "ValidateCustom"
)

const (
	PipeProto       = "Pipe"
	UnixSocketProto = "UnixSocket"
)

type ParametersDefinition map[string]ParameterMeta

type ParameterMeta struct {
	Type     string
	Default  string
	Required string
}

type TransformerSettings struct {
	Nullable           bool                       `json:"nullable,omitempty"`
	Variadic           bool                       `json:"variadic,omitempty"`
	Unique             bool                       `json:"unique,omitempty"`
	MaxLength          int64                      `json:"maxLength,omitempty"`
	SupportedTypes     []string                   `json:"supportedTypes,omitempty"`
	TransformationType domains.TransformationType `json:"transformationType,omitempty"`
}

type CustomTransformerMeta struct {
	Name       string               `json:"name,omitempty"`
	BinaryPath string               `json:"binaryPath,omitempty"`
	Engines    []string             `json:"engines,omitempty"`
	Parameters ParametersDefinition `json:"parameters,omitempty"`
	Validate   string               `json:"validate,omitempty"`
	ProvideDsn bool                 `json:"provideDsn,omitempty"`
	Settings   TransformerSettings  `json:"settings,omitempty"`
	Proto      string               `json:"proto,omitempty"`
}

func (ctm *CustomTransformerMeta) Instance() (domains.Transformer, error) {
	if ctm.Proto == PipeProto {
		return NewPipeExecTransformer(ctm), nil
	}
	return nil, errors.New("unknown proto")
}
