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

package dump

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// These are integration tests for the self-contained, schema-scoped introspection
// engine. They cover SCHEMA/DATABASE-level scoping and the introspection
// mechanics (columns, primary keys, foreign keys). TABLE-level include/exclude is
// intentionally NOT exercised here: the engine has no table-filtering input by
// construction — that concern belongs to the ObjectFilter layer (see
// pkg/common/objectfilter unit tests).

const (
	engMysqlRootUser = testutils.MysqlRootUser
	engMysqlRootPass = testutils.MysqlRootPassword
)

type txMock struct {
	*sql.Tx
}

// introspectEngineSuite runs the full introspection suite against a single
// MySQL-protocol server flavor. The body is identical across flavors; only the
// container image and the expected vendor differ, so adding a flavor is just one
// entry function below. Vendor detection is the key flavor-specific behavior —
// MySQL, MariaDB and Percona are each detected as a distinct vendor.
type introspectEngineSuite struct {
	testutils.MySQLContainerSuite
	containerImage string
	expectedVendor string
	// imagePlatform, when set, pins the container to a specific platform (e.g.
	// "linux/amd64"). Needed for flavors whose image lacks a native arm64
	// manifest (Percona) so the suite runs under Docker emulation on Apple
	// Silicon instead of failing with "no matching manifest".
	imagePlatform string
}

func (s *introspectEngineSuite) SetupSuite() {
	s.SetImage(s.containerImage)
	// The testcontainers MySQL module's default wait strategy matches a
	// MySQL-Community-specific startup line ("port: 3306  MySQL Community Server")
	// that MariaDB and Percona never emit. Override it with the flavor-agnostic
	// part of that signal: every flavor's real server binds the network port and
	// logs "port: 3306", whereas the init temp server logs "port: 0". Matching
	// "port: 3306" therefore waits for the real, externally reachable server
	// across MySQL, MariaDB and Percona ("ready for connections" is unreliable —
	// flavors emit it a different number of times, e.g. Percona's X plugin).
	s.SetContainerOptions(testcontainers.CustomizeRequestOption(
		func(req *testcontainers.GenericContainerRequest) error {
			req.WaitingFor = wait.ForLog("port: 3306").
				WithStartupTimeout(3 * time.Minute)
			if s.imagePlatform != "" {
				req.ImagePlatform = s.imagePlatform
			}
			return nil
		},
	))
	s.MySQLContainerSuite.SetupSuite()
}

func (s *introspectEngineSuite) TearDownSuite() {
	s.MySQLContainerSuite.TearDownSuite()
}

func TestIntrospectEngineMySQL(t *testing.T) {
	suite.Run(t, &introspectEngineSuite{containerImage: "mysql:8", expectedVendor: core.DBMSVendorMySQL})
}

func TestIntrospectEngineMariaDB(t *testing.T) {
	suite.Run(t, &introspectEngineSuite{containerImage: "mariadb:11", expectedVendor: core.DBMSVendorMariaDB})
}

func TestIntrospectEnginePercona(t *testing.T) {
	suite.Run(t, &introspectEngineSuite{
		containerImage: "percona:8",
		expectedVendor: core.DBMSVendorPercona,
		// percona:8 ships no arm64 manifest; pin to amd64 so it runs under
		// Docker emulation on Apple Silicon rather than failing to pull.
		imagePlatform: "linux/amd64",
	})
}

func (s *introspectEngineSuite) newEngine(inc, exc, incDB, excDB []string) *introspectEngine {
	scope, err := newSchemaScope(inc, exc, incDB, excDB)
	s.Require().NoError(err)
	return newIntrospectEngine(scope)
}

func (s *introspectEngineSuite) TestIntrospect() {
	ctx := context.Background()
	db, err := s.GetConnectionWithUser(ctx, engMysqlRootUser, engMysqlRootPass)
	s.Require().NoError(err)

	migrationUp := []string{
		`CREATE TABLE testdb.test_table_1 (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL);`,
		`CREATE TABLE testdb.test_table_2 (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL);`,
		`CREATE SCHEMA testdb1;`,
		`CREATE TABLE testdb1.test_table_3 (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL);`,
	}
	migrationDown := []string{
		`DROP TABLE testdb.test_table_1;`,
		`DROP TABLE testdb.test_table_2;`,
		`DROP TABLE testdb1.test_table_3;`,
		`DROP SCHEMA testdb1;`,
	}
	s.MigrateUp(ctx, migrationUp)
	defer s.MigrateDown(ctx, migrationDown)

	idNameColumns := []mysqlmodels.Column{
		{Name: "id", TypeName: "int", DataType: newPtr("int"), NumericPrecision: newPtr(10), NumericScale: newPtr(0), NotNull: true},
		{Name: "name", TypeName: "varchar(255)", DataType: newPtr("varchar"), NotNull: true},
	}
	t1 := mysqlmodels.Table{Name: "test_table_1", Schema: "testdb", Columns: idNameColumns}
	t2 := mysqlmodels.Table{Name: "test_table_2", Schema: "testdb", Columns: idNameColumns}
	t3 := mysqlmodels.Table{Name: "test_table_3", Schema: "testdb1", Columns: idNameColumns}

	runIntrospect := func(eng *introspectEngine) {
		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		s.Require().NoError(eng.introspect(ctx, &txMock{tx}))
	}

	s.Run("basic: all tables in all non-system schemas", func() {
		eng := s.newEngine(nil, nil, nil, nil)
		runIntrospect(eng)
		// Every table in every allowed schema is introspected; nothing is skipped
		// at the table level.
		compareTables(s.T(), []mysqlmodels.Table{t1, t2, t3}, eng.tables)
		s.ElementsMatch([]string{"testdb", "testdb1"}, eng.allowedSchemas)

		// Server version + vendor are introspected; the vendor is the key
		// flavor-discriminating assertion.
		s.NotEmpty(eng.version.FullString)
		s.Positive(eng.version.Major)
		s.Equal(s.expectedVendor, eng.version.Vendor())
	})

	s.Run("include schema returns ALL its tables (no table-level skip)", func() {
		eng := s.newEngine([]string{"testdb"}, nil, nil, nil)
		runIntrospect(eng)
		compareTables(s.T(), []mysqlmodels.Table{t1, t2}, eng.tables)
		s.Equal([]string{"testdb"}, eng.allowedSchemas)
	})

	s.Run("exclude schema", func() {
		eng := s.newEngine(nil, []string{"testdb"}, nil, nil)
		runIntrospect(eng)
		compareTables(s.T(), []mysqlmodels.Table{t3}, eng.tables)
		s.Equal([]string{"testdb1"}, eng.allowedSchemas)
	})

	s.Run("include schema by exact name", func() {
		eng := s.newEngine([]string{"testdb1"}, nil, nil, nil)
		runIntrospect(eng)
		compareTables(s.T(), []mysqlmodels.Table{t3}, eng.tables)
	})

	s.Run("include schema by regex", func() {
		// testdb[0-9] matches "testdb1" but not "testdb".
		eng := s.newEngine([]string{"testdb[0-9]"}, nil, nil, nil)
		runIntrospect(eng)
		compareTables(s.T(), []mysqlmodels.Table{t3}, eng.tables)
	})

	s.Run("exclude database (folded into schema scope)", func() {
		eng := s.newEngine(nil, nil, nil, []string{"testdb"})
		runIntrospect(eng)
		compareTables(s.T(), []mysqlmodels.Table{t3}, eng.tables)
	})

	s.Run("no matching schema errors", func() {
		eng := s.newEngine([]string{"does_not_exist"}, nil, nil, nil)
		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		err = eng.introspect(ctx, &txMock{tx})
		s.Require().ErrorIs(err, errIntrospectNoSchemasFound)
	})
}

func (s *introspectEngineSuite) TestGetPrimaryKey() {
	ctx := context.Background()
	db, err := s.GetConnectionWithUser(ctx, engMysqlRootUser, engMysqlRootPass)
	s.Require().NoError(err)

	s.Run("with pk", func() {
		s.MigrateUp(ctx, []string{`CREATE TABLE simple_table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL);`})
		defer s.MigrateDown(ctx, []string{`DROP TABLE simple_table;`})

		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		eng := s.newEngine(nil, nil, nil, nil)
		pks, err := eng.getPrimaryKey(ctx, &txMock{tx}, "testdb", "simple_table")
		s.Require().NoError(err)
		s.Require().Equal([]string{"id"}, pks)
	})

	s.Run("no pk", func() {
		s.MigrateUp(ctx, []string{`CREATE TABLE simple_table_no_pk (id INT, name VARCHAR(255) NOT NULL);`})
		defer s.MigrateDown(ctx, []string{`DROP TABLE simple_table_no_pk;`})

		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		eng := s.newEngine(nil, nil, nil, nil)
		pks, err := eng.getPrimaryKey(ctx, &txMock{tx}, "testdb", "simple_table_no_pk")
		s.Require().NoError(err)
		s.Require().Nil(pks)
	})
}

func (s *introspectEngineSuite) TestGetForeignKeys() {
	ctx := context.Background()
	db, err := s.GetConnectionWithUser(ctx, engMysqlRootUser, engMysqlRootPass)
	s.Require().NoError(err)

	s.Run("no fk", func() {
		s.MigrateUp(ctx, []string{`CREATE TABLE no_fk (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL);`})
		defer s.MigrateDown(ctx, []string{`DROP TABLE no_fk;`})

		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		eng := s.newEngine(nil, nil, nil, nil)
		refs, err := eng.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "no_fk")
		s.Require().NoError(err)
		s.Require().Empty(refs)
	})

	s.Run("single-column fk not nullable", func() {
		s.MigrateUp(ctx, []string{
			`CREATE TABLE main_table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL);`,
			`CREATE TABLE ref_table_not_null (id INT PRIMARY KEY, main_table_id INT NOT NULL, FOREIGN KEY (main_table_id) REFERENCES main_table (id));`,
		})
		defer s.MigrateDown(ctx, []string{`DROP TABLE ref_table_not_null;`, `DROP TABLE main_table;`})

		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		eng := s.newEngine(nil, nil, nil, nil)
		actual, err := eng.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "ref_table_not_null")
		s.Require().NoError(err)
		s.Require().Equal([]core.Reference{
			{
				ReferencedSchema: "testdb",
				ReferencedName:   "main_table",
				ConstraintSchema: "testdb",
				ConstraintName:   "ref_table_not_null_ibfk_1",
				IsNullable:       false,
			},
		}, actual)

		keys, err := eng.getForeignKeyKeys(ctx, &txMock{tx}, "testdb", "ref_table_not_null_ibfk_1")
		s.Require().NoError(err)
		s.Require().Equal([]string{"main_table_id"}, keys)
	})

	s.Run("unknown fk keys errors", func() {
		tx, err := db.Begin()
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		eng := s.newEngine(nil, nil, nil, nil)
		_, err = eng.getForeignKeyKeys(ctx, &txMock{tx}, "testdb", "unknown_ibfk_1")
		s.Require().ErrorIs(err, errIntrospectNoKeysFound)
	})
}

func compareTables(t *testing.T, expected, actual []mysqlmodels.Table) {
	t.Helper()
	require.Lenf(t, actual, len(expected), "expected %d tables, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equal(t, expected[i].Name, actual[i].Name)
		assert.Equal(t, expected[i].Schema, actual[i].Schema)
		compareColumns(t, expected[i].Columns, actual[i].Columns)
	}
}

func compareColumns(t *testing.T, expected, actual []mysqlmodels.Column) {
	t.Helper()
	require.Lenf(t, actual, len(expected), "expected %d columns, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equal(t, expected[i].Name, actual[i].Name)
		assert.Equal(t, expected[i].NotNull, actual[i].NotNull)
		// COLUMN_TYPE (TypeName) carries a display width that differs across
		// flavors (e.g. MySQL 8 "int" vs MariaDB "int(11)"), so assert the
		// portable DATA_TYPE instead.
		assert.Equal(t, expected[i].DataType != nil, actual[i].DataType != nil)
		if expected[i].DataType != nil && actual[i].DataType != nil {
			assert.Equal(t, *expected[i].DataType, *actual[i].DataType)
		}
	}
}

func newPtr[T any](v T) *T { return &v }
