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

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	mysqldump "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	mysqlrestore "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/restore"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

const (
	mysqlImage = "mysql:8.4"
	sourceDB   = "testdb"
	targetDB   = "restoredb"
	testDumpID = "test-dump-id"
)

var (
	migrationUpTable = []string{
		`CREATE TABLE test_table (
			id   INT          NOT NULL,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		)`,
		`CREATE TABLE other_table (
			id   INT          NOT NULL,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		)`,
	}
	migrationUpData = []string{
		`INSERT INTO test_table  (id, name) VALUES (1,'test1'), (2,'test2')`,
		`INSERT INTO other_table (id, name) VALUES (1,'other1')`,
	}
	migrationDown = []string{
		`DROP TABLE IF EXISTS test_table`,
		`DROP TABLE IF EXISTS other_table`,
	}
)

// restoreTestSuite drives the MySQL-backed restore integration tests.
// A single MySQL container is started once for the suite. Each test
// creates a throwaway `restoredb` database, runs restore into it, then
// drops it so the next test starts clean.
type restoreTestSuite struct {
	testutils.MySQLContainerSuite
	db *sql.DB // shared connection for test setup and verification
}

func (s *restoreTestSuite) SetupSuite() {
	s.MySQLContainerSuite.
		SetImage(mysqlImage).
		SetMigrationUp(append(migrationUpTable, migrationUpData...)).
		SetMigrationDown(migrationDown).
		SetupSuite()

	var err error
	s.db, err = s.GetRootConnection(context.Background())
	s.Require().NoError(err)
	s.Require().NoError(s.db.Ping())
}

func (s *restoreTestSuite) TearDownSuite() {
	if s.db != nil {
		_ = s.db.Close()
	}
	s.MySQLContainerSuite.TearDownSuite()
}

// setupInfrastructure attaches logging and a validation collector to ctx.
func (s *restoreTestSuite) setupInfrastructure(ctx context.Context) context.Context {
	ctx = log.Ctx(ctx).With().Str(core.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(core.MetaKeyEngine, "mysql")
	return validationcollector.WithCollector(ctx, vc)
}

// createRestoreTarget creates `restoredb` and the supplied tables (fully qualified DDL).
// The returned cleanup func drops the database.
func (s *restoreTestSuite) createRestoreTarget(ctx context.Context, tables []string) func() {
	_, err := s.db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+targetDB+"`")
	s.Require().NoError(err, "create restore target database")
	for _, ddl := range tables {
		_, err = s.db.ExecContext(ctx, ddl)
		s.Require().NoError(err, "create restore target table")
	}
	return func() {
		_, _ = s.db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+targetDB+"`")
	}
}

// seedTarget inserts rows into a table (used for conflict-resolution tests).
func (s *restoreTestSuite) seedTarget(ctx context.Context, query string) {
	_, err := s.db.ExecContext(ctx, query)
	s.Require().NoError(err, "seed target")
}

// runDump executes a dump (real data, mocked schema) and returns the populated storage.
func (s *restoreTestSuite) runDump(ctx context.Context, cfg *config.Config) *validate.Storage {
	st := validate.New("")

	cmdRunner := &CmdRunnerMock{}
	cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			_, err := args.Get(1).(io.Writer).Write([]byte("-- mock schema content"))
			s.Require().NoError(err)
		})
	cmdProducer := &CmdProducerMock{}
	cmdProducer.On("Produce", "mysqldump", mock.Anything, mock.Anything, mock.Anything).
		Return(cmdRunner, nil)

	dumpProcess, err := mysqldump.NewDump(
		cfg,
		registry.DefaultTransformerRegistry,
		st,
		cmdProducer,
		mysqldump.GetMySQLDumpOpts(cfg)...,
	)
	s.Require().NoError(err)
	s.Require().NoError(dumpProcess.Run(ctx))
	return st
}

// runRestore runs a restore against the container. Schema sections are handled by
// a no-op mock mysql CLI; data sections execute real INSERTs.
func (s *restoreTestSuite) runRestore(ctx context.Context, cfg *config.Config, st *validate.Storage) {
	cmdRunner := &CmdRunnerMock{}
	cmdRunner.On("ExecuteCmdAndForwardStdout", mock.Anything).Return(nil)
	cmdProducer := &CmdProducerMock{}
	cmdProducer.On("Produce", "mysql", mock.Anything, mock.Anything, mock.Anything).
		Return(cmdRunner, nil)

	restoreProcess := mysqlrestore.NewRestore(cfg, st, testDumpID, cmdProducer)
	s.Require().NoError(restoreProcess.Run(ctx))
}

// baseDumpConfig returns a fully-reset dump config for the container's sourceDB.
// Calling this always resets cfg.Dump to avoid state leaking between tests
// (config.NewConfig returns a singleton).
func (s *restoreTestSuite) baseDumpConfig(ctx context.Context) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = "mysql"
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	// Reset dump to defaults so previous test state doesn't leak.
	cfg.Dump = config.NewDump()
	opts := s.GetRootConnectionOpts(ctx)
	cfg.Dump.MysqlConfig.Host = opts.Host
	cfg.Dump.MysqlConfig.Port = opts.Port
	cfg.Dump.MysqlConfig.User = opts.User
	cfg.Dump.MysqlConfig.Password = opts.Password
	cfg.Dump.MysqlConfig.ConnectDatabase = sourceDB
	cfg.Dump.Options.IncludeSchema = []string{sourceDB}
	cfg.Dump.Options.Compress = false
	cfg.Dump.Options.Pgzip = false
	return cfg
}

// baseRestoreConfig returns a fully-reset restore config for the container,
// remapping sourceDB → targetDB.
func (s *restoreTestSuite) baseRestoreConfig(ctx context.Context) *config.Config {
	cfg := s.baseDumpConfig(ctx)

	// Fully reset restore config so InsertIgnore/InsertReplace/DataOnly etc.
	// from a prior test don't leak through the singleton.
	cfg.Restore = config.NewRestore()
	opts := s.GetRootConnectionOpts(ctx)
	cfg.Restore.MysqlConfig.Host = opts.Host
	cfg.Restore.MysqlConfig.Port = opts.Port
	cfg.Restore.MysqlConfig.User = opts.User
	cfg.Restore.MysqlConfig.Password = opts.Password
	cfg.Restore.Options.RemapDatabase = map[string]string{sourceDB: targetDB}
	cfg.Restore.Options.DatabaseReplaceMode = core.DatabaseReplaceModeStrict
	return cfg
}

// countRows returns the number of rows in `db.table`.
func (s *restoreTestSuite) countRows(ctx context.Context, db, table string) int {
	var n int
	s.Require().NoError(s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+db+"`.`"+table+"`",
	).Scan(&n))
	return n
}

// getNames fetches all `name` values from `db.table`, ordered by id.
func (s *restoreTestSuite) getNames(ctx context.Context, db, table string) []string {
	rows, err := s.db.QueryContext(ctx,
		"SELECT name FROM `"+db+"`.`"+table+"` ORDER BY id",
	)
	s.Require().NoError(err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		s.Require().NoError(rows.Scan(&n))
		names = append(names, n)
	}
	s.Require().NoError(rows.Err())
	return names
}

// targetTableExists reports whether a table exists in targetDB.
func (s *restoreTestSuite) targetTableExists(ctx context.Context, table string) bool {
	var n int
	s.Require().NoError(s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
		targetDB, table,
	).Scan(&n))
	return n > 0
}

// --- Tests ---

func (s *restoreTestSuite) TestRestore_DataOnly() {
	// data-only: pre-data and post-data sections are skipped entirely;
	// only INSERT statements from the dump are executed.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table`  (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
		"CREATE TABLE `" + targetDB + "`.`other_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	cfg := s.baseRestoreConfig(ctx)
	cfg.Restore.Options.DataOnly = true

	st := s.runDump(ctx, s.baseDumpConfig(ctx))
	defer st.Cleanup()

	s.runRestore(ctx, cfg, st)

	s.Equal(2, s.countRows(ctx, targetDB, "test_table"))
	s.ElementsMatch([]string{"test1", "test2"}, s.getNames(ctx, targetDB, "test_table"))
	s.Equal(1, s.countRows(ctx, targetDB, "other_table"))
	s.ElementsMatch([]string{"other1"}, s.getNames(ctx, targetDB, "other_table"))
}

func (s *restoreTestSuite) TestRestore_FullRestore_SchemaAndData() {
	// Full restore: schema sections run via the no-op mock mysql CLI,
	// data sections execute real INSERTs.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table`  (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
		"CREATE TABLE `" + targetDB + "`.`other_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	cfg := s.baseRestoreConfig(ctx)
	// DataOnly=false, SchemaOnly=false → all three sections run

	st := s.runDump(ctx, s.baseDumpConfig(ctx))
	defer st.Cleanup()

	s.runRestore(ctx, cfg, st)

	s.Equal(2, s.countRows(ctx, targetDB, "test_table"))
	s.ElementsMatch([]string{"test1", "test2"}, s.getNames(ctx, targetDB, "test_table"))
	s.Equal(1, s.countRows(ctx, targetDB, "other_table"))
}

func (s *restoreTestSuite) TestRestore_SchemaOnly() {
	// schema-only: data section is skipped; target tables stay empty after restore.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	cfg := s.baseRestoreConfig(ctx)
	cfg.Restore.Options.SchemaOnly = true

	st := s.runDump(ctx, s.baseDumpConfig(ctx))
	defer st.Cleanup()

	s.runRestore(ctx, cfg, st)

	s.Equal(0, s.countRows(ctx, targetDB, "test_table"), "schema-only must not insert rows")
}

func (s *restoreTestSuite) TestRestore_TableInclude() {
	// Dump only test_table; other_table must not appear in the target at all.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	dumpCfg := s.baseDumpConfig(ctx)
	dumpCfg.Dump.Options.IncludeTable = []string{sourceDB + ".test_table"}

	st := s.runDump(ctx, dumpCfg)
	defer st.Cleanup()

	// baseRestoreConfig resets cfg.Dump (same singleton), but the dump already ran.
	cfg := s.baseRestoreConfig(ctx)
	cfg.Restore.Options.DataOnly = true

	s.runRestore(ctx, cfg, st)

	s.Equal(2, s.countRows(ctx, targetDB, "test_table"))
	s.False(s.targetTableExists(ctx, "other_table"), "other_table must not exist in target")
}

func (s *restoreTestSuite) TestRestore_InsertIgnore() {
	// Pre-seed target with id=1; INSERT IGNORE must leave that row untouched
	// and insert only the non-conflicting row (id=2).
	// Dump only test_table to keep the target simple.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	s.seedTarget(ctx, "INSERT INTO `"+targetDB+"`.`test_table` (id, name) VALUES (1, 'existing')")

	dumpCfg := s.baseDumpConfig(ctx)
	dumpCfg.Dump.Options.IncludeTable = []string{sourceDB + ".test_table"}

	st := s.runDump(ctx, dumpCfg)
	defer st.Cleanup()

	cfg := s.baseRestoreConfig(ctx)
	cfg.Restore.Options.DataOnly = true
	cfg.Restore.MysqlConfig.InsertIgnore = true

	s.runRestore(ctx, cfg, st)

	s.Equal(2, s.countRows(ctx, targetDB, "test_table"))

	var name string
	s.Require().NoError(s.db.QueryRowContext(ctx,
		"SELECT name FROM `"+targetDB+"`.`test_table` WHERE id = 1",
	).Scan(&name))
	s.Equal("existing", name, "INSERT IGNORE must not overwrite conflicting row")
}

func (s *restoreTestSuite) TestRestore_InsertReplace() {
	// Pre-seed target with id=1; INSERT REPLACE must overwrite that row
	// with the value from the dump.
	// Dump only test_table to keep the target simple.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	s.seedTarget(ctx, "INSERT INTO `"+targetDB+"`.`test_table` (id, name) VALUES (1, 'stale')")

	dumpCfg := s.baseDumpConfig(ctx)
	dumpCfg.Dump.Options.IncludeTable = []string{sourceDB + ".test_table"}

	st := s.runDump(ctx, dumpCfg)
	defer st.Cleanup()

	cfg := s.baseRestoreConfig(ctx)
	cfg.Restore.Options.DataOnly = true
	cfg.Restore.MysqlConfig.InsertReplace = true

	s.runRestore(ctx, cfg, st)

	s.Equal(2, s.countRows(ctx, targetDB, "test_table"))

	var name string
	s.Require().NoError(s.db.QueryRowContext(ctx,
		"SELECT name FROM `"+targetDB+"`.`test_table` WHERE id = 1",
	).Scan(&name))
	s.Equal("test1", name, "INSERT REPLACE must overwrite conflicting row with dump value")
}

func (s *restoreTestSuite) TestRestore_DatabaseRemap() {
	// Verify remap-database redirects rows to the remapped DB while leaving
	// the source DB untouched.
	ctx := s.setupInfrastructure(context.Background())

	cleanup := s.createRestoreTarget(ctx, []string{
		"CREATE TABLE `" + targetDB + "`.`test_table`  (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
		"CREATE TABLE `" + targetDB + "`.`other_table` (id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
	})
	defer cleanup()

	cfg := s.baseRestoreConfig(ctx)
	cfg.Restore.Options.DataOnly = true

	st := s.runDump(ctx, s.baseDumpConfig(ctx))
	defer st.Cleanup()

	s.runRestore(ctx, cfg, st)

	s.Equal(2, s.countRows(ctx, sourceDB, "test_table"), "source must be unchanged")
	s.Equal(2, s.countRows(ctx, targetDB, "test_table"), "target must have restored rows")
}

// TestRestore_Scripts exercises every combination of section (pre-data / data /
// post-data), event (before / after), and script type (sql / sql-file /
// command) as a table-driven suite of sub-tests.
//
// SQL and sql-file scripts insert a unique marker row into a `script_log`
// tracking table that is pre-created in the target database; command scripts
// create a temp file via `touch`.  After restore each sub-test asserts the
// expected side-effect is present, proving the script actually executed.
//
// Section / DataOnly interaction:
//   - pre-data and post-data scripts run only when DataOnly=false (the
//     processor must enter those sections).  The MySQL CLI is mocked so no
//     real DDL is applied; tables are pre-created by the test helper.
//   - data scripts are tested with DataOnly=true for simplicity.
func (s *restoreTestSuite) TestRestore_Scripts() {
	type scriptKind string
	const (
		kindSQL     scriptKind = "sql"
		kindSQLFile scriptKind = "sql-file"
		kindCommand scriptKind = "command"
	)

	tests := []struct {
		name    string
		section core.DumpSection
		when    core.ScriptEventType
		kind    scriptKind
	}{
		// pre-data section
		{name: "pre_data_before_sql", section: core.DumpSectionPreData, when: core.ScriptEventTypeBefore, kind: kindSQL},
		{name: "pre_data_after_sql", section: core.DumpSectionPreData, when: core.ScriptEventTypeAfter, kind: kindSQL},
		{name: "pre_data_before_sql_file", section: core.DumpSectionPreData, when: core.ScriptEventTypeBefore, kind: kindSQLFile},
		{name: "pre_data_after_sql_file", section: core.DumpSectionPreData, when: core.ScriptEventTypeAfter, kind: kindSQLFile},
		{name: "pre_data_before_command", section: core.DumpSectionPreData, when: core.ScriptEventTypeBefore, kind: kindCommand},
		{name: "pre_data_after_command", section: core.DumpSectionPreData, when: core.ScriptEventTypeAfter, kind: kindCommand},
		// data section
		{name: "data_before_sql", section: core.DumpSectionData, when: core.ScriptEventTypeBefore, kind: kindSQL},
		{name: "data_after_sql", section: core.DumpSectionData, when: core.ScriptEventTypeAfter, kind: kindSQL},
		{name: "data_before_sql_file", section: core.DumpSectionData, when: core.ScriptEventTypeBefore, kind: kindSQLFile},
		{name: "data_after_sql_file", section: core.DumpSectionData, when: core.ScriptEventTypeAfter, kind: kindSQLFile},
		{name: "data_before_command", section: core.DumpSectionData, when: core.ScriptEventTypeBefore, kind: kindCommand},
		{name: "data_after_command", section: core.DumpSectionData, when: core.ScriptEventTypeAfter, kind: kindCommand},
		// post-data section
		{name: "post_data_before_sql", section: core.DumpSectionPostData, when: core.ScriptEventTypeBefore, kind: kindSQL},
		{name: "post_data_after_sql", section: core.DumpSectionPostData, when: core.ScriptEventTypeAfter, kind: kindSQL},
		{name: "post_data_before_sql_file", section: core.DumpSectionPostData, when: core.ScriptEventTypeBefore, kind: kindSQLFile},
		{name: "post_data_after_sql_file", section: core.DumpSectionPostData, when: core.ScriptEventTypeAfter, kind: kindSQLFile},
		{name: "post_data_before_command", section: core.DumpSectionPostData, when: core.ScriptEventTypeBefore, kind: kindCommand},
		{name: "post_data_after_command", section: core.DumpSectionPostData, when: core.ScriptEventTypeAfter, kind: kindCommand},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			ctx := s.setupInfrastructure(context.Background())

			// Pre-create target DB: script_log for SQL/file scripts plus the
			// regular data tables so data restore and mocked schema both succeed.
			cleanup := s.createRestoreTarget(ctx, []string{
				"CREATE TABLE `" + targetDB + "`.`script_log` " +
					"(id INT AUTO_INCREMENT PRIMARY KEY, marker VARCHAR(255) NOT NULL)",
				"CREATE TABLE `" + targetDB + "`.`test_table` " +
					"(id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
				"CREATE TABLE `" + targetDB + "`.`other_table` " +
					"(id INT NOT NULL, name VARCHAR(255) NOT NULL, PRIMARY KEY(id))",
			})
			defer cleanup()

			cfg := s.baseRestoreConfig(ctx)
			// DataOnly skips pre-data/post-data sections entirely; only use it
			// for data-section scripts where schema processing is irrelevant.
			cfg.Restore.Options.DataOnly = tc.section == core.DumpSectionData

			var scr core.Script
			var markerFile string // populated only for kindCommand

			switch tc.kind {
			case kindSQL:
				// Use fully-qualified table name: the script connection has no
				// default database (ConnectDatabase is unset in the restore config).
				scr = core.Script{
					Name:    tc.name,
					Section: tc.section,
					When:    tc.when,
					Query: fmt.Sprintf(
						"INSERT INTO `%s`.`script_log` (marker) VALUES ('%s')",
						targetDB, tc.name,
					),
				}

			case kindSQLFile:
				dir := s.T().TempDir()
				path := filepath.Join(dir, "script.sql")
				content := fmt.Sprintf(
					"INSERT INTO `%s`.`script_log` (marker) VALUES ('%s')",
					targetDB, tc.name,
				)
				s.Require().NoError(os.WriteFile(path, []byte(content), 0600))
				scr = core.Script{
					Name:      tc.name,
					Section:   tc.section,
					When:      tc.when,
					QueryFile: path,
				}

			case kindCommand:
				dir := s.T().TempDir()
				markerFile = filepath.Join(dir, tc.name+".marker")
				scr = core.Script{
					Name:    tc.name,
					Section: tc.section,
					When:    tc.when,
					Command: []string{"touch", markerFile},
				}
			}

			cfg.Restore.Scripts = []core.Script{scr}

			st := s.runDump(ctx, s.baseDumpConfig(ctx))
			defer st.Cleanup()

			s.runRestore(ctx, cfg, st)

			switch tc.kind {
			case kindSQL, kindSQLFile:
				var count int
				s.Require().NoError(s.db.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM `"+targetDB+"`.`script_log` WHERE marker = ?",
					tc.name,
				).Scan(&count))
				s.Equal(1, count, "script must insert exactly one marker row into script_log")

			case kindCommand:
				_, err := os.Stat(markerFile)
				s.NoError(err, "command script must create the marker file")
			}
		})
	}
}

func TestRestoreSuite(t *testing.T) {
	suite.Run(t, new(restoreTestSuite))
}
