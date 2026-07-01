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

// Package remap_e2e contains end-to-end integration tests for the database
// remap-database feature.
//
// The tests exercise the full dump → restore pipeline against a real MySQL 8.4
// container.  Each case dumps one or more source databases and then restores
// them using a remap-database mapping, verifying that rows land in the correct
// target database and that strict / relaxed / empty-mode semantics are
// enforced.
package remap_e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	mysqlcmddump "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	mysqlrestore "github.com/greenmaskio/greenmask/pkg/mysql/restore"
	"github.com/greenmaskio/greenmask/pkg/storages/directory"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// ---------------------------------------------------------------------------
// Source / target database names used across all test cases.
// ---------------------------------------------------------------------------

const (
	mysqlImage = "mysql:8.4"

	// Source databases created on the MySQL container.
	srcDB     = "src_db"
	anotherDB = "another_db"

	// Common target names used in remapping scenarios.
	dstDB = "dst_db"
	altDB = "alt_db"
)

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

type RemapSuite struct {
	testutils.MySQLContainerSuite
}

func (s *RemapSuite) SetupSuite() {
	if _, err := exec.LookPath("mysqldump"); err != nil {
		s.T().Skip("mysqldump not found in PATH — skipping remap e2e tests")
	}
	if _, err := exec.LookPath("mysql"); err != nil {
		s.T().Skip("mysql not found in PATH — skipping remap e2e tests")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		s.T().Skip("Docker is not available — skipping remap e2e tests")
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(zerolog.DebugLevel)

	s.MySQLContainerSuite.
		SetImage(mysqlImage).
		SetMigrationUp([]string{
			`CREATE DATABASE IF NOT EXISTS ` + srcDB,
			`CREATE TABLE IF NOT EXISTS ` + srcDB + `.users (
				id   INT NOT NULL AUTO_INCREMENT,
				name VARCHAR(255) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB`,
			`INSERT INTO ` + srcDB + `.users (name) VALUES ('alice'), ('bob'), ('carol')`,

			`CREATE DATABASE IF NOT EXISTS ` + anotherDB,
			`CREATE TABLE IF NOT EXISTS ` + anotherDB + `.items (
				id    INT NOT NULL AUTO_INCREMENT,
				label VARCHAR(255) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB`,
			`INSERT INTO ` + anotherDB + `.items (label) VALUES ('item1'), ('item2')`,
		}).
		SetupSuite()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *RemapSuite) setupCtx(ctx context.Context, cfg *config.Config) context.Context {
	s.Require().NoError(utils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format))
	ctx = log.Ctx(ctx).With().Str(core.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(core.MetaKeyEngine, "mysql")
	return validationcollector.WithCollector(ctx, vc)
}

func (s *RemapSuite) baseConfig(ctx context.Context) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = core.DBMSEngineMySQL
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	opts := s.GetRootConnectionOpts(ctx)
	cfg.Dump.MysqlConfig.Host = opts.Host
	cfg.Dump.MysqlConfig.Port = opts.Port
	cfg.Dump.MysqlConfig.User = opts.User
	cfg.Dump.MysqlConfig.Password = opts.Password
	cfg.Dump.MysqlConfig.ConnectDatabase = srcDB
	cfg.Dump.MysqlConfig.VendorOptions = []string{"--add-drop-table"}
	cfg.Dump.Options.Compression = core.CompressionNone

	cfg.Restore.MysqlConfig.Host = opts.Host
	cfg.Restore.MysqlConfig.Port = opts.Port
	cfg.Restore.MysqlConfig.User = opts.User
	cfg.Restore.MysqlConfig.Password = opts.Password
	cfg.Restore.MysqlConfig.ConnectDatabase = srcDB
	cfg.Restore.Options.CreateDatabase = true
	cfg.Restore.Options.IfNotExists = true

	return cfg
}

func (s *RemapSuite) runDump(ctx context.Context, cfg *config.Config, dumpDir string, schemas []string) core.DumpID {
	cfg.Dump.Options.IncludeSchema = schemas
	dirSt, err := directory.New(directory.NewDirectoryConfig(dumpDir))
	s.Require().NoError(err, "create dump storage")

	d, err := mysqlcmddump.NewDump(
		cfg,
		registry.DefaultTransformerRegistry,
		dirSt,
		utils.NewDefaultCmdProducer(),
		mysqlcmddump.GetMySQLDumpOpts(cfg)...,
	)
	s.Require().NoError(err, "new dump")
	s.Require().NoError(d.Run(ctx), "run dump")
	return d.GetDumpID()
}

func (s *RemapSuite) runRestore(ctx context.Context, cfg *config.Config, dumpDir string, dumpID core.DumpID) error {
	// The restore pipeline provisions its own storage from cfg.Storage, so point
	// it at the same directory the dump wrote to.
	cfg.Storage.Type = "directory"
	cfg.Storage.Directory.Path = dumpDir

	pipeline, err := mysqlrestore.NewRestorePipeline(utils.NewDefaultCmdProducer())
	s.Require().NoError(err, "create restore pipeline")
	_, err = pipeline.RunRestore(ctx, *cfg, dumpID)
	return err
}

// countRows queries the given table in the given database using a root
// connection and returns the row count.
func (s *RemapSuite) countRows(ctx context.Context, db *sql.DB, database, table string) int {
	var n int
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", database, table))
	s.Require().NoError(row.Scan(&n))
	return n
}

// dropDatabases drops the listed databases if they exist.  It is called before
// each sub-test so target databases from a previous run do not pollute results.
func (s *RemapSuite) dropDatabases(ctx context.Context, db *sql.DB, names ...string) {
	for _, name := range names {
		_, err := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
		s.Require().NoErrorf(err, "drop database %s", name)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// rowCheck describes an expected row count in a specific database and table.
type rowCheck struct {
	database string
	table    string
	wantRows int
}

func (s *RemapSuite) TestDatabaseRemap() {
	tests := []struct {
		name        string
		dumpSchemas []string
		remap       map[string]string
		mode        core.DatabaseReplacementMode
		dropBefore  []string
		wantErr     bool
		checks      []rowCheck
	}{
		{
			name:        "strict_all_mapped",
			dumpSchemas: []string{srcDB, anotherDB},
			remap:       map[string]string{srcDB: dstDB, anotherDB: altDB},
			mode:        core.DatabaseReplaceModeStrict,
			dropBefore:  []string{dstDB, altDB},
			checks: []rowCheck{
				{database: dstDB, table: "users", wantRows: 3},
				{database: altDB, table: "items", wantRows: 2},
			},
		},
		{
			// anotherDB has no mapping — strict mode must reject before any SQL.
			name:        "strict_missing_entry_fails",
			dumpSchemas: []string{srcDB, anotherDB},
			remap:       map[string]string{srcDB: dstDB},
			mode:        core.DatabaseReplaceModeStrict,
			dropBefore:  []string{dstDB},
			wantErr:     true,
		},
		{
			// anotherDB has no mapping — relaxed mode keeps it under its original name.
			name:        "relaxed_partial_map",
			dumpSchemas: []string{srcDB, anotherDB},
			remap:       map[string]string{srcDB: dstDB},
			mode:        core.DatabaseReplaceModeRelaxed,
			dropBefore:  []string{dstDB},
			checks: []rowCheck{
				{database: dstDB, table: "users", wantRows: 3},
				{database: anotherDB, table: "items", wantRows: 2},
			},
		},
		{
			// Empty mode defaults to strict; single dump database is fully covered.
			name:        "empty_mode_defaults_to_strict_single_db",
			dumpSchemas: []string{srcDB},
			remap:       map[string]string{srcDB: dstDB},
			mode:        "",
			dropBefore:  []string{dstDB},
			checks: []rowCheck{
				{database: dstDB, table: "users", wantRows: 3},
			},
		},
		{
			// Empty mode defaults to strict; unmapped second database must fail.
			name:        "empty_mode_defaults_to_strict_missing_entry_fails",
			dumpSchemas: []string{srcDB, anotherDB},
			remap:       map[string]string{srcDB: dstDB},
			mode:        "",
			dropBefore:  []string{dstDB},
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			ctx := context.Background()
			cfg := s.baseConfig(ctx)
			ctx = s.setupCtx(ctx, cfg)

			db, err := s.GetRootConnection(ctx)
			s.Require().NoError(err)
			defer db.Close()

			s.dropDatabases(ctx, db, tc.dropBefore...)

			dumpDir := s.T().TempDir()
			dumpID := s.runDump(ctx, cfg, dumpDir, tc.dumpSchemas)

			cfg.Restore.Options.RemapDatabase = tc.remap
			cfg.Restore.Options.DatabaseReplaceMode = tc.mode

			err = s.runRestore(ctx, cfg, dumpDir, dumpID)
			if tc.wantErr {
				s.Require().Error(err)
				return
			}
			s.Require().NoError(err)

			for _, chk := range tc.checks {
				got := s.countRows(ctx, db, chk.database, chk.table)
				s.Equalf(chk.wantRows, got, "row count mismatch in %s.%s", chk.database, chk.table)
			}
		})
	}
}

func TestRemapSuite(t *testing.T) {
	suite.Run(t, new(RemapSuite))
}
