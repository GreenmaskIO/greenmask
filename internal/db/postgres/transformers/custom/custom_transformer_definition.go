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

package custom

import (
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	JsonModeName = "json"
	CsvModeName  = "csv"
	TextModeName = "text"
)

type TransformerDefinition struct {
	Name                     string                `mapstructure:"name" yaml:"name" json:"name"`
	Description              string                `mapstructure:"description" yaml:"description" json:"description"`
	Executable               string                `mapstructure:"executable" yaml:"executable" json:"executable"`
	Args                     []string              `mapstructure:"args" yaml:"args" json:"args"`
	Parameters               []*toolkit.Parameter  `mapstructure:"parameters" yaml:"parameters" json:"parameters"`
	Validate                 bool                  `mapstructure:"validate" yaml:"validate" json:"validate"`
	AutoDiscover             bool                  `mapstructure:"auto_discover" yaml:"auto_discover" json:"auto_discover"`
	ValidationTimeout        time.Duration         `mapstructure:"validation_timeout" yaml:"validation_timeout" json:"validation_timeout"`
	AutoDiscoveryTimeout     time.Duration         `mapstructure:"auto_discovery_timeout" yaml:"auto_discovery_timeout" json:"auto_discovery_timeout"`
	RowTransformationTimeout time.Duration         `mapstructure:"row_transformation_timeout" yaml:"row_transformation_timeout" json:"row_transformation_timeout"`
	ExpectedExitCode         int                   `mapstructure:"expected_exit_code" yaml:"expected_exit_code" json:"expected_exit_code"`
	Driver                   *toolkit.DriverParams `mapstructure:"driver" yaml:"driver" json:"driver"`
}
