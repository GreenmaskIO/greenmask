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

// Package connconfig defines RestoreConnectionConfig — the MySQL-specific
// ConnectionConfigurer for the restore pipeline. It lives in its own package so
// that the factory-level packages (table, schema) can import it for a concrete
// type assertion without creating an import cycle with pkg/mysql/restore.
package connconfig

import (
	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	"github.com/greenmaskio/greenmask/pkg/config"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/opts"
)

// RestoreConnectionConfig is the MySQL-specific ConnectionConfigurer for the
// restore pipeline. It carries the target-DB connection parameters from the
// restore config section (cfg.Restore.*), not the dump source.
type RestoreConnectionConfig struct {
	Common             config.CommonRestoreOptions
	MySQL              config.MysqlRestoreConfig
	ConnectionPoolSize int
}

func (c *RestoreConnectionConfig) ConnectionConfig() any { return c }

// SchemaRestoreParams returns the mysql CLI connection/auth flags plus any
// user-specified vendor options.
func (c *RestoreConnectionConfig) SchemaRestoreParams(ssl commonconfig.SSLOpts) ([]string, error) {
	return c.MySQL.SchemaRestoreParams(ssl)
}

// Env returns the process environment for mysql CLI invocations.
func (c *RestoreConnectionConfig) Env() ([]string, error) {
	return c.MySQL.Env()
}

// SchemaRestoreOptions returns the parameters used by MysqlSchemaRestorer.
func (c *RestoreConnectionConfig) SchemaRestoreOptions() opts.SchemaRestoreOpts {
	return opts.SchemaRestoreOpts{
		SSL:            c.Common.SSL,
		CreateDatabase: c.Common.CreateDatabase,
		IfNotExists:    c.Common.IfNotExists,
		RemapDatabase:  c.Common.RemapDatabase,
	}
}

// TableRestoreOptions returns the parameters used by InsertRestoreWriter and CsvRestoreWriter.
func (c *RestoreConnectionConfig) TableRestoreOptions() opts.TableRestoreOpts {
	return opts.TableRestoreOpts{
		PrintWarnings:           c.MySQL.PrintWarnings,
		MaxFetchWarnings:        c.MySQL.MaxFetchWarnings,
		DisableForeignKeyChecks: c.MySQL.DisableForeignKeyChecks,
		DisableUniqueChecks:     c.MySQL.DisableUniqueChecks,
		InsertIgnore:            c.MySQL.InsertIgnore,
		InsertReplace:           c.MySQL.InsertReplace,
		MaxInsertStatementSize:  c.MySQL.MaxInsertStatementSize,
		RemapDatabase:           c.Common.RemapDatabase,
	}
}

// CsvConnConfig returns a ConnConfig for opening a go-sql-driver/mysql connection
// needed by the CSV writer (LOAD DATA LOCAL INFILE requires the standard driver).
func (c *RestoreConnectionConfig) CsvConnConfig() (*mysqlmodels.ConnConfig, error) {
	return c.MySQL.ConnectionConfig(c.Common.SSL)
}
