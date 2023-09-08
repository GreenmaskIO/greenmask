package transformers

import (
	"fmt"

	"golang.org/x/exp/slices"
)

const (
	ErrorValidationSeverity   = "error"
	WarningValidationSeverity = "warning"
	InfoValidationSeverity    = "info"
	DebugValidationSeverity   = "debug"
)

type Trace struct {
	SchemaName      string `json:"schemaName,omitempty"`
	TableName       string `json:"tableName,omitempty"`
	TransformerName string `json:"transformerName,omitempty"`
	ParameterName   string `json:"parameterName,omitempty"`
	Msg             string `json:"msg,omitempty"`
}

type ValidationWarnings []*ValidationWarning

func (re ValidationWarnings) IsFatal() bool {
	return slices.ContainsFunc(re, func(warning *ValidationWarning) bool {
		return warning.Level == ErrorValidationSeverity
	})
}

type ValidationWarning struct {
	Msg   string                 `json:"msg,omitempty"`
	Level string                 `json:"level,omitempty"`
	Trace *Trace                 `json:"trace,omitempty"`
	Meta  map[string]interface{} `json:"meta,omitempty"`
}

func NewValidationWarning() *ValidationWarning {
	return &ValidationWarning{
		Level: WarningValidationSeverity,
		Meta:  make(map[string]interface{}),
	}
}

func (re *ValidationWarning) SetMsg(msg string) *ValidationWarning {
	re.Msg = msg
	return re
}

func (re *ValidationWarning) SetMsgf(msg string, args ...any) *ValidationWarning {
	re.Msg = fmt.Sprintf(msg, args...)
	return re
}

func (re *ValidationWarning) SetLevel(level string) *ValidationWarning {
	re.Level = level
	return re
}

func (re *ValidationWarning) AddMeta(key string, value interface{}) *ValidationWarning {
	re.Meta[key] = value
	return re
}

func (re *ValidationWarning) SetTrace(value *Trace) *ValidationWarning {
	re.Trace = value
	return re
}
