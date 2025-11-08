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

package cmd

import (
	"fmt"
)

type RowDriverName string

const (
	RowDriverNameJson RowDriverName = "json"
	RowDriverNameCSV  RowDriverName = "csv"
	RowDriverNameText RowDriverName = "text"
)

var (
	errInvalidRowDriver = fmt.Errorf("invalid row driver")
)

func (m RowDriverName) Validate() error {
	switch m {
	case RowDriverNameJson, RowDriverNameCSV, RowDriverNameText:
		return nil
	default:
		return fmt.Errorf(`value "%s": %w`, m, errInvalidRowDriver)
	}
}

type JsonRowDriverDataFormat string

const (
	JsonRowDriverDataFormatBytes JsonRowDriverDataFormat = "bytes"
	JsonRowDriverDataFormatText  JsonRowDriverDataFormat = "text"
)

var errInvalidJsonRowDriverDataFormat = fmt.Errorf("invalid json row driver transferRecord format")

func (m JsonRowDriverDataFormat) Validate() error {
	switch m {
	case JsonRowDriverDataFormatBytes, JsonRowDriverDataFormatText:
		return nil
	default:
		return fmt.Errorf(`value "%s": %w`, m, errInvalidJsonRowDriverDataFormat)
	}
}

type JsonRowDriverColumnFormat string

const (
	JsonRowDriverColumnFormatByIndexes JsonRowDriverColumnFormat = "indexes"
	JsonRowDriverColumnFormatByNames   JsonRowDriverColumnFormat = "names"
)

var errInvalidJsonRowDriverColumnFormat = fmt.Errorf("invalid json row driver column format")

func (m JsonRowDriverColumnFormat) Validate() error {
	switch m {
	case JsonRowDriverColumnFormatByIndexes, JsonRowDriverColumnFormatByNames:
		return nil
	default:
		return fmt.Errorf(`value "%s": %w`, m, errInvalidJsonRowDriverColumnFormat)
	}
}

type JsonRowDriverConfig struct {
	DataFormat   JsonRowDriverDataFormat   `json:"data_format,omitempty"`
	ColumnFormat JsonRowDriverColumnFormat `json:"column_format,omitempty"`
}

func (cfg *JsonRowDriverConfig) Validate() error {
	if err := cfg.DataFormat.Validate(); err != nil {
		return fmt.Errorf("transferRecord format: %w", err)
	}
	if err := cfg.ColumnFormat.Validate(); err != nil {
		return fmt.Errorf("column format: %w", err)
	}
	return nil
}

type RowDriverSetting struct {
	Name       RowDriverName       `json:"name"`
	JsonConfig JsonRowDriverConfig `json:"json,omitempty"`
}

var DefaultRowDriverParams = RowDriverSetting{
	Name: RowDriverNameJson,
	JsonConfig: JsonRowDriverConfig{
		DataFormat:   JsonRowDriverDataFormatText,
		ColumnFormat: JsonRowDriverColumnFormatByIndexes,
	},
}

// Validate - validate driver params and set default values if needed
func (m *RowDriverSetting) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return fmt.Errorf("validate name: %w", err)
	}

	if m.Name == RowDriverNameJson {
		if err := m.JsonConfig.Validate(); err != nil {
			return fmt.Errorf("validate json config: %w", err)
		}
	}
	return nil
}

func (m *RowDriverSetting) IsPositionedAttributeFormat() bool {
	return m.Name == RowDriverNameCSV ||
		(m.Name == RowDriverNameJson && m.JsonConfig.ColumnFormat == JsonRowDriverColumnFormatByIndexes)
}
