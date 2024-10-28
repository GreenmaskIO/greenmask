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

var DefaultRowDriverParams = DriverParams{
	Name:                 JsonModeName,
	JsonDataFormat:       JsonBytesDataFormatName,
	JsonAttributesFormat: JsonAttributesIndexesFormatName,
	CsvAttributesFormat:  CsvAttributesDirectNumeratingFormatName,
}

type DriverParams struct {
	Name                 string `json:"name"`
	JsonDataFormat       string `json:"json_data_format,omitempty"`
	JsonAttributesFormat string `json:"json_attributes_format,omitempty"`
	CsvAttributesFormat  string `json:"csv_attributes_format,omitempty"`
}

// Validate - validate driver params and set default values if needed
func (dp *DriverParams) Validate() error {
	if dp.Name != JsonModeName && dp.Name != CsvModeName && dp.Name != TextModeName {
		return fmt.Errorf(`unexpected driver name "%s"`, dp.Name)
	}

	switch dp.Name {
	case JsonModeName:
		return dp.validateJson()
	case CsvModeName:
		return dp.validateCsv()
	}

	return nil
}

func (dp *DriverParams) validateCsv() error {
	if dp.CsvAttributesFormat != CsvAttributesDirectNumeratingFormatName && dp.CsvAttributesFormat != CsvAttributesConfigNumeratingFormatName {
		if dp.CsvAttributesFormat == "" {
			dp.CsvAttributesFormat = CsvAttributesDirectNumeratingFormatName
		} else {
			return fmt.Errorf(`unexpected csv_attributes_format "%s"`, dp.CsvAttributesFormat)
		}
	}
	return nil
}

func (dp *DriverParams) validateJson() error {
	if dp.JsonDataFormat != JsonBytesDataFormatName && dp.JsonDataFormat != JsonTextDataFormatName {
		if dp.JsonDataFormat == "" {
			dp.JsonDataFormat = JsonBytesDataFormatName
		} else {
			return fmt.Errorf(`unexpected format "%s"`, dp.JsonDataFormat)
		}
	}
	if dp.JsonAttributesFormat != JsonAttributesNamesFormatName && dp.JsonAttributesFormat != JsonAttributesIndexesFormatName {
		if dp.JsonAttributesFormat == "" {
			dp.JsonAttributesFormat = JsonAttributesIndexesFormatName
		} else {
			return fmt.Errorf(`unexpected json_attributes_format "%s"`, dp.JsonAttributesFormat)
		}
	}
	return nil
}

type TransformerDefinition struct {
	Name             string                 `json:"name"`
	Description      string                 `json:"description"`
	Parameters       []*ParameterDefinition `json:"parameters"`
	Validate         bool                   `json:"validate"`
	ExpectedExitCode int                    `json:"expected_exit_code"`
	Driver           *DriverParams          `json:"driver"`
	New              NewTransformerFunc     `json:"-"`
}

func NewTransformerDefinition(name string, makeFunc NewTransformerFunc) *TransformerDefinition {
	return &TransformerDefinition{
		Name:   name,
		New:    makeFunc,
		Driver: &DefaultRowDriverParams,
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

func (d *TransformerDefinition) SetMode(v *DriverParams) *TransformerDefinition {
	if err := v.Validate(); err != nil {
		panic(err)
	}
	d.Driver = v
	return d
}
