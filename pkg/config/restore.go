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
	"fmt"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlcommonconfig "github.com/greenmaskio/greenmask/pkg/mysql/config"
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

const DefaultMaxFetchWarnings = 10

const DefaultMaxInsertStatementSize = 4 * 1024 * 1024

type MysqlRestoreConfig struct {
	mysqlcommonconfig.ConnectionOpts `mapstructure:",squash" json:",squash,omitempty"` //nolint:staticcheck
	VendorOptions                    []string                                          `mapstructure:"vendor-options" yaml:"vendor-options" json:"vendor-options,omitempty"`
	PrintWarnings                    bool                                              `mapstructure:"print-warnings" yaml:"print-warnings" json:"print_warnings"`
	// MaxFetchWarnings - the maximum number of warnings to fetch and print. If 0, all warnings are printed.
	MaxFetchWarnings        int  `mapstructure:"max-fetch-warnings" yaml:"max-fetch-warnings" json:"max_fetch_warnings"`
	DisableForeignKeyChecks bool `mapstructure:"disable-fk-checks" yaml:"disable-fk-checks" json:"disable_fk_checks"`
	DisableUniqueChecks     bool `mapstructure:"disable-unique-checks" yaml:"disable-unique-checks" json:"disable_unique_checks"`
	// MaxInsertStatementSize controls the maximum byte size of a single batched INSERT statement.
	MaxInsertStatementSize int  `mapstructure:"max-insert-statement-size" yaml:"max-insert-statement-size" json:"max_insert_statement_size"`
	InsertIgnore           bool `mapstructure:"insert-ignore" yaml:"insert-ignore" json:"insert_ignore"`
	InsertReplace          bool `mapstructure:"insert-replace" yaml:"insert-replace" json:"insert_replace"`
}

func (r *MysqlRestoreConfig) Validate() error {
	if r.InsertIgnore && r.InsertReplace {
		return fmt.Errorf("insert-ignore and insert-replace are mutually exclusive")
	}
	return nil
}

func (r *MysqlRestoreConfig) SchemaRestoreParams(ssl commonconfig.SSLOpts) ([]string, error) {
	params := r.Params(ssl)
	params = append(params, r.VendorOptions...)
	return params, nil
}

type PostgresqlRestoreConfig struct {
	Options any `mapstructure:"options" yaml:"options" json:"options"`
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
	// SingleTransaction restores the whole dump inside one transaction per
	// connection, committed only on full success and rolled back on any error
	// (mirrors pg_restore's --single-transaction).
	SingleTransaction bool `mapstructure:"single-transaction" yaml:"single-transaction" json:"single-transaction"`
	// CreateDatabase controls whether greenmask issues CREATE DATABASE statements before restoring schema.
	CreateDatabase bool `mapstructure:"create-database" yaml:"create-database" json:"create_database"`
	// IfNotExists adds IF NOT EXISTS to CREATE DATABASE and (in future) other object creation statements.
	IfNotExists bool `mapstructure:"if-not-exists" yaml:"if-not-exists" json:"if_not_exists"`
	// RemapDatabase maps original database names to new names (original → renamed).
	RemapDatabase map[string]string `mapstructure:"remap-database" yaml:"remap-database" json:"remap-database,omitempty"`
	// DatabaseReplaceMode controls mapping strictness: "strict" (default) requires all databases in the dump
	// to have a mapping entry; "relaxed" renames only the listed databases and keeps the rest as-is.
	DatabaseReplaceMode core.DatabaseReplacementMode `mapstructure:"database-replace-mode" yaml:"database-replace-mode" json:"database-replace-mode,omitempty"`
	Section             []string                     `mapstructure:"section"               yaml:"section"               json:"section,omitempty"`
	SSL                 commonconfig.SSLOpts         `mapstructure:",squash"               json:",squash,omitempty"` //nolint:staticcheck
}

func (o *CommonRestoreOptions) Validate() error {
	for _, s := range o.Section {
		if _, ok := knownRestoreSections[s]; !ok {
			return fmt.Errorf("unknown section %q: must be one of pre-data, data, post-data", s)
		}
	}

	if o.DatabaseReplaceMode != "" {
		if err := o.DatabaseReplaceMode.Validate(); err != nil {
			return fmt.Errorf("invalid database replace mode: %w", err)
		}
	}
	return nil
}

var knownRestoreSections = map[string]struct{}{
	"pre-data":  {},
	"data":      {},
	"post-data": {},
}

type Restore struct {
	Options          CommonRestoreOptions           `mapstructure:"options" yaml:"options" json:"options"`
	MysqlConfig      MysqlRestoreConfig             `mapstructure:"mysql" yaml:"mysql"`
	PostgresqlConfig PostgresqlRestoreConfig        `mapstructure:"postgresql" yaml:"postgresql"`
	Scripts          []core.Script                  `mapstructure:"scripts" yaml:"scripts" json:"scripts,omitempty"`
	ErrorExclusions  DataRestorationErrorExclusions `mapstructure:"insert_error_exclusions" yaml:"insert_error_exclusions" json:"insert_error_exclusions,omitempty"`
}

func (r *Restore) Validate() error {
	if err := r.Options.Validate(); err != nil {
		return fmt.Errorf("validate options: %w", err)
	}
	if err := r.MysqlConfig.Validate(); err != nil {
		return fmt.Errorf("validate mysql config: %w", err)
	}
	for i, script := range r.Scripts {
		if err := script.Validate(); err != nil {
			return fmt.Errorf("validate script #%d name '%s': %w", i, script.Name, err)
		}
	}
	return nil
}

func NewRestore() Restore {
	return Restore{
		MysqlConfig: MysqlRestoreConfig{
			ConnectionOpts: mysqlcommonconfig.ConnectionOpts{
				MaxAllowedPacket: mysqlcommonconfig.DefaultMaxAllowedPacket,
			},
			MaxFetchWarnings:       DefaultMaxFetchWarnings,
			MaxInsertStatementSize: DefaultMaxInsertStatementSize,
		},
		Options: CommonRestoreOptions{
			RemapDatabase:       make(map[string]string),
			DatabaseReplaceMode: core.DatabaseReplaceModeStrict,
		},
	}
}
