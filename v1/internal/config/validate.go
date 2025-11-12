// Copyright 2025 Greenmask
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

package config

type Validate struct {
	Tables           []string `mapstructure:"table" yaml:"tables" json:"tables,omitempty"`
	Data             bool     `mapstructure:"data" yaml:"data" json:"data,omitempty"`
	Diff             bool     `mapstructure:"diff" yaml:"diff" json:"diff,omitempty"`
	Schema           bool     `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	RowsLimit        int      `mapstructure:"rows-limit" yaml:"rows-limit" json:"rows-limit,omitempty"`
	ResolvedWarnings []string `mapstructure:"resolved-warnings" yaml:"resolved-warnings" json:"resolved-warnings,omitempty"`
	TableFormat      string   `mapstructure:"table-format" yaml:"table-format" json:"table-format,omitempty"`
	Format           string   `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	OnlyTransformed  bool     `mapstructure:"transformed-only" yaml:"transformed-only" json:"transformed-only,omitempty"`
	Warnings         bool     `mapstructure:"warnings" yaml:"warnings" json:"warnings,omitempty"`
}
