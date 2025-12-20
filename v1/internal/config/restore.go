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

import (
	pgconfig "github.com/greenmaskio/greenmask/v1/internal/pg/restore/config"

	mysqlconfig "github.com/greenmaskio/greenmask/v1/internal/mysql/restore/config"
)

type TablesDataRestorationErrorExclusions struct {
	Name        string   `mapstructure:"name" yaml:"name" json:"name,omitempty"`
	Schema      string   `mapstructure:"schema" yaml:"schema" json:"schema,omitempty"`
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type GlobalDataRestorationErrorExclusions struct {
	Constraints []string `mapstructure:"constraints" yaml:"constraints" json:"constraints,omitempty"`
	ErrorCodes  []string `mapstructure:"error_codes" yaml:"error_codes" json:"error_codes,omitempty"`
}

type DataRestorationErrorExclusions struct {
	Tables []TablesDataRestorationErrorExclusions `mapstructure:"tables" yaml:"tables" json:"tables,omitempty"`
	Global GlobalDataRestorationErrorExclusions   `mapstructure:"global" yaml:"global" json:"global,omitempty"`
}

type Script struct {
	Name      string   `mapstructure:"name"`
	When      string   `mapstructure:"when"`
	Query     string   `mapstructure:"query"`
	QueryFile string   `mapstructure:"query_file"`
	Command   []string `mapstructure:"command"`
}

type MysqlRestoreConfig struct {
	Options mysqlconfig.RestoreOptions `mapstructure:"options" yaml:"options" json:"options"`
}

type PostgresqlRestoreConfig struct {
	Options pgconfig.RestoreOptions `mapstructure:"options" yaml:"options" json:"options"`
}

type CommonRestoreOptions struct {
	//IncludeTable     []string `mapstructure:"include-table" yaml:"include-table" json:"include-table"`
	//ExcludeTable     []string `mapstructure:"exclude-table" yaml:"exclude-table" json:"exclude-table"`
	//IncludeSchema    []string `mapstructure:"include-schema" yaml:"include-schema" json:"include-schema"`
	//ExcludeSchema    []string `mapstructure:"exclude-schema" yaml:"exclude-schema" json:"exclude-schema"`
	//ExcludeTableData []string `mapstructure:"exclude-table-data" yaml:"exclude-table-data" json:"exclude-table-data"`
	DataOnly       bool `mapstructure:"data-only" yaml:"data-only" json:"data-only"`
	SchemaOnly     bool `mapstructure:"schema-only" yaml:"schema-only" json:"schema-only"`
	Jobs           int  `mapstructure:"jobs" yaml:"jobs" json:"jobs"`
	RestoreInOrder bool `mapstructure:"restore-in-order" yaml:"restore-in-order" json:"restore-in-order"`
}

type Restore struct {
	Options          CommonRestoreOptions           `mapstructure:"options" yaml:"options" json:"options"`
	MysqlConfig      MysqlRestoreConfig             `mapstructure:"mysql" yaml:"mysql"`
	PostgresqlConfig PostgresqlRestoreConfig        `mapstructure:"postgresql" yaml:"postgresql"`
	Scripts          map[string][]Script            `mapstructure:"scripts" yaml:"scripts" json:"scripts,omitempty"`
	ErrorExclusions  DataRestorationErrorExclusions `mapstructure:"insert_error_exclusions" yaml:"insert_error_exclusions" json:"insert_error_exclusions,omitempty"`
}

func NewRestore() Restore {
	return Restore{}
}
