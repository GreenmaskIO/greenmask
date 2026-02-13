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
	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/pkg/mysql/config"
	"github.com/greenmaskio/greenmask/v1/pkg/mysql/models"
)

type DumpOptions struct {
	config.ConnectionOpts `mapstructure:",squash" json:",squash,omitempty"`
	// General dump options
	AllDatabases      bool     `mapstructure:"all-databases" json:"all_databases,omitempty"`           // Dump all databases (--all-databases)
	Databases         []string `mapstructure:"databases" json:"databases,omitempty"`                   // List of databases to dump
	NoCreateInfo      bool     `mapstructure:"no-create-info" json:"no_create_info,omitempty"`         // Exclude CREATE TABLE statements (--no-create-info)
	NoData            bool     `mapstructure:"no-data" json:"no_data,omitempty"`                       // Exclude data from dump (--no-data)
	AddDropTable      bool     `mapstructure:"add-drop-table" json:"add_drop_table,omitempty"`         // Include DROP TABLE statements (--add-drop-table)
	Compact           bool     `mapstructure:"compact" json:"compact,omitempty"`                       // Reduce dump size with minimal comments (--compact)
	SkipComments      bool     `mapstructure:"skip-comments" json:"skip_comments,omitempty"`           // Do not include comments in dump (--skip-comments)
	SingleTransaction bool     `mapstructure:"single-transaction" json:"single_transaction,omitempty"` // Use a single transaction for the dump (--single-transaction)
	Quick             bool     `mapstructure:"quick" json:"quick,omitempty"`                           // Fetch rows one at a time (--quick)
	LockTables        bool     `mapstructure:"lock-tables" json:"lock_tables,omitempty"`               // Lock all tables during dump (--lock-tables)

	// Tablespace and metadata options
	NoTablespaces bool `mapstructure:"no-tablespaces" json:"no_tablespaces,omitempty"` // Exclude tablespace information (--no-tablespaces)
}

func (d *DumpOptions) GetIncludedTables() []string {
	return nil
}

func (d *DumpOptions) GetExcludedTables() []string {
	return nil
}

func (d *DumpOptions) GetExcludedSchemas() []string {
	return nil
}

func (d *DumpOptions) GetIncludedSchemas() []string {
	if len(d.Databases) > 0 {
		return d.Databases
	}
	return nil
}

func (d *DumpOptions) Env() ([]string, error) {
	return d.ConnectionOpts.Env()
}

func (d *DumpOptions) SchemaDumpParams() ([]string, error) {
	args := d.ConnectionOpts.Params()
	args = append(args, "--no-data")
	if d.AddDropTable {
		args = append(args, "--add-drop-table")
	}
	if d.Compact {
		args = append(args, "--compact")
	}
	if d.SkipComments {
		args = append(args, "--skip-comments")
	}
	if d.SingleTransaction {
		args = append(args, "--single-transaction")
	}
	if d.LockTables {
		args = append(args, "--lock-tables")
	}
	if d.NoTablespaces {
		args = append(args, "--no-tablespaces")
	}
	if len(d.Databases) > 0 {
		args = append(args, "--databases")
		args = append(args, d.Databases...)
	}
	if d.AllDatabases {
		args = append(args, "--all-databases")
	}
	return args, nil
}

func (d *DumpOptions) Get(key string) (any, error) {
	panic("not implemented")
}

func (d *DumpOptions) ConnectionConfig() (interfaces.ConnectionConfigurator, error) {
	return &models.ConnConfig{
		User:     d.User,
		Password: d.Password,
		Host:     d.Host,
		Port:     d.Port,
		Database: d.ConnectDatabase,
	}, nil
}
