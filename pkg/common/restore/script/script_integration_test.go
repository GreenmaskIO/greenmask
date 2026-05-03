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

package script

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/testutils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	integrationMigrationUp   = `CREATE TABLE script_test (id INT PRIMARY KEY, value VARCHAR(255))`
	integrationMigrationDown = `DROP TABLE IF EXISTS script_test`
)

// dbTxExec adapts a *sql.DB into a TxExec function.
func dbTxExec(db *sql.DB) TxExec {
	return func(ctx context.Context, query string) error {
		_, err := db.ExecContext(ctx, query)
		return err
	}
}

type scriptIntegrationSuite struct {
	testutils.MySQLContainerSuite
	db *sql.DB
}

func (s *scriptIntegrationSuite) SetupSuite() {
	s.SetMigrationUp([]string{integrationMigrationUp}).
		SetMigrationDown([]string{integrationMigrationDown})
	s.MySQLContainerSuite.SetupSuite()

	var err error
	s.db, err = s.GetConnection(context.Background())
	s.Require().NoError(err, "get db connection")
	s.Require().NoError(s.db.Ping(), "ping db")
}

func (s *scriptIntegrationSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
	s.MySQLContainerSuite.TearDownSuite()
}

// --- executeQuery ---

func (s *scriptIntegrationSuite) TestExecuteQuery_Success() {
	ctx := context.Background()
	e := NewExecutor(commonmodels.Script{
		Name:  "insert-row",
		Query: "INSERT INTO script_test (id, value) VALUES (1, 'hello')",
	})
	err := e.executeQuery(ctx, dbTxExec(s.db))
	s.Require().NoError(err)

	var val string
	s.Require().NoError(s.db.QueryRowContext(ctx, "SELECT value FROM script_test WHERE id = 1").Scan(&val))
	s.Equal("hello", val)

	_, _ = s.db.ExecContext(ctx, "DELETE FROM script_test WHERE id = 1")
}

func (s *scriptIntegrationSuite) TestExecuteQuery_InvalidSQL_Error() {
	ctx := context.Background()
	e := NewExecutor(commonmodels.Script{Name: "bad", Query: "THIS IS NOT SQL"})
	err := e.executeQuery(ctx, dbTxExec(s.db))
	s.Require().Error(err)
	s.Contains(err.Error(), "execute script name='bad'")
}

// --- executeQueryFile ---

func (s *scriptIntegrationSuite) TestExecuteQueryFile_Success() {
	ctx := context.Background()
	dir := s.T().TempDir()
	path := filepath.Join(dir, "insert.sql")
	require.NoError(s.T(), os.WriteFile(path, []byte("INSERT INTO script_test (id, value) VALUES (2, 'from-file')"), 0600))

	e := NewExecutor(commonmodels.Script{Name: "file-script", QueryFile: path})
	err := e.executeQueryFile(ctx, dbTxExec(s.db))
	s.Require().NoError(err)

	var val string
	s.Require().NoError(s.db.QueryRowContext(ctx, "SELECT value FROM script_test WHERE id = 2").Scan(&val))
	s.Equal("from-file", val)

	_, _ = s.db.ExecContext(ctx, "DELETE FROM script_test WHERE id = 2")
}

func (s *scriptIntegrationSuite) TestExecuteQueryFile_FileNotFound() {
	ctx := context.Background()
	e := NewExecutor(commonmodels.Script{Name: "s", QueryFile: "/nonexistent/missing.sql"})
	err := e.executeQueryFile(ctx, dbTxExec(s.db))
	s.Require().Error(err)
	s.Contains(err.Error(), "cannot open script file")
}

func (s *scriptIntegrationSuite) TestExecuteQueryFile_InvalidSQL_Error() {
	ctx := context.Background()
	dir := s.T().TempDir()
	path := filepath.Join(dir, "bad.sql")
	require.NoError(s.T(), os.WriteFile(path, []byte("NOT VALID SQL"), 0600))

	e := NewExecutor(commonmodels.Script{Name: "bad-file", QueryFile: path})
	err := e.executeQueryFile(ctx, dbTxExec(s.db))
	s.Require().Error(err)
	s.Contains(err.Error(), "execute script name='bad-file'")
}

// --- Scheduler.Exec with a real DB ---

func (s *scriptIntegrationSuite) TestScheduler_Exec_OnlyMatchingScriptsRun() {
	ctx := context.Background()

	scripts := []commonmodels.Script{
		{
			Name:    "skip-wrong-section",
			Section: commonmodels.DumpSectionPostData,
			When:    commonmodels.ScriptEventTypeBefore,
			Query:   "INSERT INTO script_test (id, value) VALUES (10, 'should-not-run')",
		},
		{
			Name:    "run-match",
			Section: commonmodels.DumpSectionPreData,
			When:    commonmodels.ScriptEventTypeBefore,
			Query:   "INSERT INTO script_test (id, value) VALUES (11, 'should-run')",
		},
	}

	sched := NewScheduler(scripts)
	err := sched.Exec(ctx, dbTxExec(s.db), commonmodels.DumpSectionPreData, commonmodels.ScriptEventTypeBefore)
	s.Require().NoError(err)

	var count int
	s.Require().NoError(s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM script_test WHERE id = 10").Scan(&count))
	s.Equal(0, count, "non-matching script must not have run")

	s.Require().NoError(s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM script_test WHERE id = 11").Scan(&count))
	s.Equal(1, count, "matching script must have run")

	_, _ = s.db.ExecContext(ctx, "DELETE FROM script_test WHERE id IN (10, 11)")
}

func (s *scriptIntegrationSuite) TestScheduler_Exec_ErrorPropagated() {
	ctx := context.Background()

	scripts := []commonmodels.Script{
		{
			Name:    "fail",
			Section: commonmodels.DumpSectionData,
			When:    commonmodels.ScriptEventTypeAfter,
			Query:   "INVALID SQL STATEMENT",
		},
	}

	sched := NewScheduler(scripts)
	err := sched.Exec(ctx, dbTxExec(s.db), commonmodels.DumpSectionData, commonmodels.ScriptEventTypeAfter)
	s.Require().Error(err)
	s.Contains(err.Error(), "execute script #0")
}

func (s *scriptIntegrationSuite) TestScheduler_Exec_MultipleMatchingScripts() {
	ctx := context.Background()

	scripts := []commonmodels.Script{
		{
			Name:    "first",
			Section: commonmodels.DumpSectionPostData,
			When:    commonmodels.ScriptEventTypeAfter,
			Query:   "INSERT INTO script_test (id, value) VALUES (20, 'first')",
		},
		{
			Name:    "second",
			Section: commonmodels.DumpSectionPostData,
			When:    commonmodels.ScriptEventTypeAfter,
			Query:   "INSERT INTO script_test (id, value) VALUES (21, 'second')",
		},
	}

	sched := NewScheduler(scripts)
	err := sched.Exec(ctx, dbTxExec(s.db), commonmodels.DumpSectionPostData, commonmodels.ScriptEventTypeAfter)
	s.Require().NoError(err)

	var count int
	s.Require().NoError(s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM script_test WHERE id IN (20, 21)").Scan(&count))
	s.Equal(2, count)

	_, _ = s.db.ExecContext(ctx, "DELETE FROM script_test WHERE id IN (20, 21)")
}

// --- Executor.Exec dispatch integration ---

func (s *scriptIntegrationSuite) TestExecutorExec_QueryDispatch() {
	ctx := context.Background()
	called := false
	exec := TxExec(func(_ context.Context, q string) error {
		called = true
		_, err := s.db.ExecContext(ctx, q)
		return err
	})
	e := NewExecutor(commonmodels.Script{
		Name:  "dispatch-query",
		Query: "INSERT INTO script_test (id, value) VALUES (30, 'dispatch')",
	})
	s.Require().NoError(e.Exec(ctx, exec))
	s.True(called)
	_, _ = s.db.ExecContext(ctx, "DELETE FROM script_test WHERE id = 30")
}

func (s *scriptIntegrationSuite) TestExecutorExec_ErrNothingToExecute() {
	ctx := context.Background()
	e := NewExecutor(commonmodels.Script{Name: "empty"})
	err := e.Exec(ctx, dbTxExec(s.db))
	s.Require().Error(err)
	s.True(errors.Is(err, errNothingToExecute))
}

func TestScriptIntegration(t *testing.T) {
	suite.Run(t, new(scriptIntegrationSuite))
}
