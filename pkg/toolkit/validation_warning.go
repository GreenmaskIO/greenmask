// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package toolkit

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"slices"
)

const (
	ErrorValidationSeverity   = "error"
	WarningValidationSeverity = "warning"
	InfoValidationSeverity    = "info"
	DebugValidationSeverity   = "debug"
)

// deprecated
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
		return warning.Severity == ErrorValidationSeverity
	})
}

type ValidationWarning struct {
	Msg      string         `json:"msg,omitempty"`
	Severity string         `json:"severity,omitempty"`
	Trace    *Trace         `json:"trace,omitempty"`
	Meta     map[string]any `json:"meta,omitempty"`
	Hash     string         `json:"hash"`
}

func NewValidationWarning() *ValidationWarning {
	return &ValidationWarning{
		Severity: WarningValidationSeverity,
		Meta:     make(map[string]interface{}),
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

func (re *ValidationWarning) SetSeverity(severity string) *ValidationWarning {
	re.Severity = severity
	return re
}

func (re *ValidationWarning) AddMeta(key string, value any) *ValidationWarning {
	re.Meta[key] = value
	return re
}

func (re *ValidationWarning) SetTrace(value *Trace) *ValidationWarning {
	re.Trace = value
	return re
}

func (re *ValidationWarning) MakeHash() {
	var meta string
	keys := make([]string, 0, len(re.Meta))

	for key := range re.Meta {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		meta = fmt.Sprintf("%s %s=%s", meta, key, re.Meta[key])
	}

	signature := fmt.Sprintf("msg=%s severity=%s %s", re.Msg, re.Severity, meta)

	hash := md5.Sum([]byte(signature))
	re.Hash = hex.EncodeToString(hash[:])
}
