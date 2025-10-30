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
	Tables           []string `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Data             bool     `mapstructure:"data" yaml:"data" json:"data,omitempty"`
	Diff             bool     `mapstructure:"diff" yaml:"diff" json:"diff,omitempty"`
	Schema           bool     `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	RowsLimit        uint64   `mapstructure:"rows_limit" yaml:"rows_limit" json:"rows_limit,omitempty"`
	ResolvedWarnings []string `mapstructure:"resolved_warnings" yaml:"resolved_warnings" json:"resolved_warnings,omitempty"`
	TableFormat      string   `mapstructure:"table_format" yaml:"table_format" json:"table_format,omitempty"`
	Format           string   `mapstructure:"format" yaml:"format" json:"format,omitempty"`
	OnlyTransformed  bool     `mapstructure:"transformed_only" yaml:"transformed_only" json:"transformed_only,omitempty"`
	Warnings         bool     `mapstructure:"warnings" yaml:"warnings" json:"warnings,omitempty"`
}
