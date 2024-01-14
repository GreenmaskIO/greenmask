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
	"fmt"
)

const (
	JsonModeName = "json"
	CsvModeName  = "csv"
	TextModeName = "text"
)

var DefaultRowDriverParams = &RowDriverParams{
	Name: CsvModeName,
	Params: map[string]interface{}{
		"format": CsvModeName,
	},
}

type RowDriverParams struct {
	Name   string                 `json:"name,omitempty"`
	Params map[string]interface{} `json:"params,omitempty"`
}

type TransformerDefinition struct {
	Name             string                 `json:"name"`
	Description      string                 `json:"description"`
	Parameters       []*ParameterDefinition `json:"parameters"`
	Validate         bool                   `json:"validate"`
	ExpectedExitCode int                    `json:"expected_exit_code"`
	Driver           *RowDriverParams       `json:"driver"`
	New              NewTransformerFunc     `json:"-"`
}

func NewTransformerDefinition(name string, makeFunc NewTransformerFunc) *TransformerDefinition {
	return &TransformerDefinition{
		Name:   name,
		New:    makeFunc,
		Driver: DefaultRowDriverParams,
	}
}

func (d *TransformerDefinition) SetDescription(v string) *TransformerDefinition {
	d.Description = v
	return d
}

func (d *TransformerDefinition) AddParameter(v *ParameterDefinition) *TransformerDefinition {
	if v == nil {
		panic("parameter is nil")
	}
	d.Parameters = append(d.Parameters, v)
	return d
}

func (d *TransformerDefinition) SetValidate(v bool) *TransformerDefinition {
	d.Validate = v
	return d
}

func (d *TransformerDefinition) SetExpectedExitCode(v int) *TransformerDefinition {
	d.ExpectedExitCode = v
	return d
}

func (d *TransformerDefinition) SetMode(v *RowDriverParams) *TransformerDefinition {
	if v == nil {
		panic("value is nil")
	}
	if v.Name != JsonModeName && v.Name != CsvModeName && v.Name != TextModeName {
		panic(fmt.Errorf(`unexpected mode "%s"`, v))
	}
	d.Driver = v
	return d
}
