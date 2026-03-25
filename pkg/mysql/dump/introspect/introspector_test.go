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

package introspect

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	models2 "github.com/greenmaskio/greenmask/pkg/mysql/models"
	"github.com/greenmaskio/greenmask/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	mysqlRootUser = testutils.MysqlRootUser
	mysqlRootPass = testutils.MysqlRootPassword
)

var systemSchemas = []string{"information_schema", "performance_schema", "mysql", "sys"}

type optMock struct {
	mock.Mock
}

func (o *optMock) GetIncludedTables() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetExcludedTables() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetExcludedSchemas() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetIncludedSchemas() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetIncludedTableData() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetExcludedTableData() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetIncludedTableDefinitions() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetExcludedTableDefinitions() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetExcludedDatabases() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func (o *optMock) GetIncludedDatabases() []string {
	res := o.Called().Get(0)
	if res == nil {
		return nil
	}
	return res.([]string)
}

func newDefaultOptMock() *optMock {
	m := &optMock{}
	m.On("GetIncludedTables").Return(nil).Maybe()
	m.On("GetExcludedTables").Return(nil).Maybe()
	m.On("GetExcludedSchemas").Return(nil).Maybe()
	m.On("GetIncludedSchemas").Return(nil).Maybe()
	m.On("GetExcludedDatabases").Return(nil).Maybe()
	m.On("GetIncludedDatabases").Return(nil).Maybe()
	m.On("GetIncludedTableData").Return(nil).Maybe()
	m.On("GetExcludedTableData").Return(nil).Maybe()
	m.On("GetIncludedTableDefinitions").Return(nil).Maybe()
	m.On("GetExcludedTableDefinitions").Return(nil).Maybe()
	return m
}

type txMock struct {
	*sql.Tx
}

type mysqlSuite struct {
	testutils.MySQLContainerSuite
}

//func (s *mysqlSuite) SetupSuite() {
//	s.SetMigrationUser(mysqlRootUser, mysqlRootPass).
//		SetRootUser(mysqlRootUser, mysqlRootPass).
//		SetupSuite()
//}

func (s *mysqlSuite) TearDownSuite() {
	s.MySQLContainerSuite.TearDownSuite()
}

func (s *mysqlSuite) TestMySQLContainerSuite() {
	s.Require().NotNil(s.Container)
}

func TestMySQL(t *testing.T) {
	suite.Run(t, new(mysqlSuite))
}

func (s *mysqlSuite) TestIntrospector_Introspect() {
	ctx := context.Background()
	db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
	s.Require().NoError(err)

	migrationUp := []string{
		`
			CREATE TABLE testdb.test_table_1 (
					id INT PRIMARY KEY AUTO_INCREMENT,
					name VARCHAR(255) NOT NULL
			);
		`,
		`
			CREATE TABLE testdb.test_table_2 (
					id INT PRIMARY KEY AUTO_INCREMENT,
					name VARCHAR(255) NOT NULL
			);
		`,
		`
			CREATE SCHEMA testdb1;
		`,
		`
			CREATE TABLE testdb1.test_table_3 (
					id INT PRIMARY KEY AUTO_INCREMENT,
					name VARCHAR(255) NOT NULL
    	 	);
        `,
	}

	migrationDown := []string{
		`DROP TABLE testdb.test_table_1;`,
		`DROP TABLE testdb.test_table_2;`,
		`DROP TABLE testdb1.test_table_3;`,
		`DROP SCHEMA testdb1;`,
	}
	s.MigrateUp(ctx, migrationUp)
	defer s.MigrateDown(ctx, migrationDown)

	s.Run("basic", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetIncludedSchemas").Return(nil)

		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		expected := []models2.Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
			{
				Name:   "test_table_2",
				Schema: "testdb",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
			{
				Name:   "test_table_3",
				Schema: "testdb1",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
		}

		compareTables(s.T(), expected, i.tables)
	})

	s.Run("filter by include tables", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return([]string{"testdb.test_table_1"})
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetIncludedSchemas").Return(nil)

		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		expected := []models2.Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
		}

		compareTables(s.T(), expected, i.tables)
	})

	s.Run("filter by exclude tables", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return([]string{"testdb.test_table_1"})
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetIncludedSchemas").Return(nil)

		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		expected := []models2.Table{
			{
				Name:   "test_table_2",
				Schema: "testdb",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
			{
				Name:   "test_table_3",
				Schema: "testdb1",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
		}
		compareTables(s.T(), expected, i.tables)
	})

	s.Run("filter by exclude schemas", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(append([]string{"testdb"}, systemSchemas...))
		opt.On("GetIncludedSchemas").Return(nil)

		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		expected := []models2.Table{
			{
				Name:   "test_table_3",
				Schema: "testdb1",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
		}
		compareTables(s.T(), expected, i.tables)
	})

	s.Run("filter by include schemas", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetIncludedSchemas").Return([]string{"testdb1"})

		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		expected := []models2.Table{
			{
				Name:   "test_table_3",
				Schema: "testdb1",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
		}
		compareTables(s.T(), expected, i.tables)
	})

	s.Run("filter by regex", func() {
		opt := &optMock{}
		// Schema pattern
		opt.On("GetIncludedSchemas").Return([]string{"testdb.*"})
		// Table pattern (fqtn: schema.table)
		opt.On("GetIncludedTables").Return([]string{`.*\.test_table_1`})
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		// Should match testdb.test_table_1 and testdb1.test_table_1 (if exists)
		// Based on migrationUp in Setup:
		// test_table_1 in testdb
		// test_table_2 in testdb
		// test_table_3 in testdb1
		// So should match test_table_1 in testdb.
		expected := []models2.Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []models2.Column{
					{
						Name:              "id",
						TypeName:          "int",
						DataType:          newPtr("int"),
						NumericPrecision:  newPtr(10),
						NumericScale:      newPtr(0),
						DateTimePrecision: nil,
						NotNull:           true,
					},
					{
						Name:              "name",
						TypeName:          "varchar(255)",
						DataType:          newPtr("varchar"),
						NumericPrecision:  nil,
						NumericScale:      nil,
						DateTimePrecision: nil,
						NotNull:           true,
					},
				},
			},
		}
		compareTables(s.T(), expected, i.tables)
	})

	s.Run("filter by include table schema", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetIncludedSchemas").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return([]string{"testdb.test_table_1"})
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		// test_table_1: should have NeedDumpSchema=true, NeedDumpData=false
		foundTable1 := false
		for _, t := range i.tables {
			if t.Name == "test_table_1" && t.Schema == "testdb" {
				foundTable1 = true
				s.True(t.NeedDumpSchema)
				s.False(t.NeedDumpData)
			}
		}
		s.True(foundTable1)
		s.Len(i.tables, 1)
	})

	s.Run("complex filters", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return([]string{"testdb.test_table_1", "testdb1.test_table_3"})
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(systemSchemas)
		opt.On("GetIncludedSchemas").Return([]string{"testdb", "testdb1"})
		opt.On("GetIncludedTableDefinitions").Return([]string{"testdb.test_table_2"})
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		err = i.Introspect(ctx, &txMock{tx})
		s.Require().NoError(err)

		// testdb.test_table_1: NeedDumpSchema=true, NeedDumpData=true
		// testdb.test_table_2: NeedDumpSchema=true, NeedDumpData=false
		// testdb1.test_table_3: NeedDumpSchema=true, NeedDumpData=true

		s.Len(i.tables, 3)
		for _, t := range i.tables {
			if t.Schema == "testdb" && t.Name == "test_table_1" {
				s.True(t.NeedDumpSchema)
				s.True(t.NeedDumpData)
			} else if t.Schema == "testdb" && t.Name == "test_table_2" {
				s.True(t.NeedDumpSchema)
				s.False(t.NeedDumpData)
			} else if t.Schema == "testdb1" && t.Name == "test_table_3" {
				s.True(t.NeedDumpSchema)
				s.True(t.NeedDumpData)
			}
		}
	})
}

func (s *mysqlSuite) TestIntrospector_GetSchemaRelatedSettings() {

	s.Run("inclusion only", func() {
		opt := &optMock{}
		opt.On("GetIncludedSchemas").Return([]string{"testdb"})
		opt.On("GetExcludedSchemas").Return(nil)
		opt.On("GetIncludedTables").Return([]string{"testdb.test_table_1"})
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableData").Return(nil)
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)

		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		i.allSchemas = []string{"testdb", "mysql", "sys"}
		i.tables = []models2.Table{
			{Schema: "testdb", Name: "test_table_1", NeedDumpSchema: true, NeedDumpData: true},
			{Schema: "testdb", Name: "test_table_2", NeedDumpSchema: false, NeedDumpData: true},
		}
		var err2 error
		i.tm, err2 = newObjectMatcher(opt)
		s.Require().NoError(err2)

		settings := i.GetSchemaRelatedSettings()

		s.Equal([]string{"testdb"}, settings.AllowedSchemas)

		s.Equal(map[string][]string{"testdb": {"test_table_1"}}, settings.IncludeTables)
		s.Empty(settings.ExcludeTables["testdb"])

		s.Equal(map[string][]string{"testdb": {"test_table_1", "test_table_2"}}, settings.IncludeTableData)
		s.Empty(settings.ExcludeTableData["testdb"])
	})

	s.Run("exclusion only", func() {
		opt := &optMock{}
		opt.On("GetIncludedSchemas").Return(nil)
		opt.On("GetExcludedSchemas").Return([]string{"mysql", "sys"})
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return([]string{"testdb.test_table_2"})
		opt.On("GetIncludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableDefinitions").Return(nil)
		opt.On("GetExcludedTableData").Return([]string{"testdb.test_table_2"})
		opt.On("GetIncludedTableData").Return(nil)
		opt.On("GetIncludedDatabases").Return(nil)
		opt.On("GetExcludedDatabases").Return(nil)

		i, err := NewIntrospector(opt)
		s.Require().NoError(err)
		i.allSchemas = []string{"testdb", "mysql", "sys"}
		i.tables = []models2.Table{
			{Schema: "testdb", Name: "test_table_1", NeedDumpSchema: true, NeedDumpData: true},
		}
		i.excludedTables = []models2.Table{
			{Schema: "testdb", Name: "test_table_2", NeedDumpSchema: false, NeedDumpData: false},
		}
		var err2 error
		i.tm, err2 = newObjectMatcher(opt)
		s.Require().NoError(err2)

		settings := i.GetSchemaRelatedSettings()

		s.Equal([]string{"testdb"}, settings.AllowedSchemas)

		s.Empty(settings.IncludeTables["testdb"])
		s.Equal(map[string][]string{"testdb": {"test_table_2"}}, settings.ExcludeTables)

		s.Empty(settings.IncludeTableData["testdb"])
		s.Equal(map[string][]string{"testdb": {"test_table_2"}}, settings.ExcludeTableData)
	})
}

func (s *mysqlSuite) TestIntrospector_getPrimaryKey() {
	ctx := context.Background()
	_, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
	s.Require().NoError(err)

	s.Run("basic", func() {
		migrationUp := []string{
			`
			CREATE TABLE simple_table
			(
				id   INT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			);
			`,
		}

		migrationDown := []string{
			`DROP TABLE simple_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		pks, err := i.getPrimaryKey(ctx, &txMock{tx}, "testdb", "simple_table")
		s.Require().Equal([]string{"id"}, pks)
		s.Require().NoError(err)
	})

	s.Run("no pks", func() {
		migrationUp := []string{
			`
			CREATE TABLE simple_table_with_no_pk
			(
				id   INT,
				name VARCHAR(255) NOT NULL
			);
			`,
		}

		migrationDown := []string{
			`DROP TABLE simple_table_with_no_pk;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		pks, err := i.getPrimaryKey(ctx, &txMock{tx}, "testdb", "simple_table_with_no_pk")
		s.Require().Nil(pks)
		s.Require().NoError(err)
	})
}

func (s *mysqlSuite) TestIntrospector_getForeignKeyConstraints() {
	ctx := context.Background()
	_, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
	s.Require().NoError(err)

	s.Run("table with no fks", func() {
		migrationUp := []string{
			`
			CREATE TABLE simple_table_with_no_fk
			(
				id   INT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			);
			`,
		}

		migrationDown := []string{
			`DROP TABLE simple_table_with_no_fk;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		refs, err := i.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "simple_table_with_no_fk")
		s.Require().NoError(err)
		s.Require().Emptyf(refs, "expected no foreign keys, got %v", refs)
	})

	s.Run("one column fk not nullable", func() {
		migrationUp := []string{
			`
			CREATE TABLE simple_main_table
			(
				id   INT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			);
		`,
			`
			CREATE TABLE simple_ref_table_not_nullable
			(
				id   					INT PRIMARY KEY,
				simple_main_table_id 	INT NOT NULL ,
				FOREIGN KEY (simple_main_table_id) REFERENCES simple_main_table (id)
			);
		`,
		}

		migrationDown := []string{
			`DROP TABLE simple_ref_table_not_nullable;`,
			`DROP TABLE simple_main_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		actual, err := i.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "simple_ref_table_not_nullable")
		s.Require().NoError(err)
		expected := []models.Reference{
			{
				ReferencedSchema: "testdb",
				ReferencedName:   "simple_main_table",
				ConstraintSchema: "testdb",
				ConstraintName:   "simple_ref_table_not_nullable_ibfk_1",
				IsNullable:       false,
			},
		}
		s.Require().Equal(expected, actual)
	})

	s.Run("one column fk nullable", func() {
		migrationUp := []string{
			`
			CREATE TABLE simple_main_table
			(
				id   INT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			);
		`,
			`
			CREATE TABLE simple_ref_table_nullable
			(
				id   					INT PRIMARY KEY,
				simple_main_table_id 	INT,
				FOREIGN KEY (simple_main_table_id) REFERENCES simple_main_table (id)
			);
		`,
		}

		migrationDown := []string{
			`DROP TABLE simple_ref_table_nullable;`,
			`DROP TABLE simple_main_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		actual, err := i.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "simple_ref_table_nullable")
		s.Require().NoError(err)
		expected := []models.Reference{
			{
				ReferencedSchema: "testdb",
				ReferencedName:   "simple_main_table",
				ConstraintSchema: "testdb",
				ConstraintName:   "simple_ref_table_nullable_ibfk_1",
				IsNullable:       true,
			},
		}
		s.Require().Equal(expected, actual)
	})

	s.Run("two column fk not nullable", func() {
		migrationUp := []string{
			`
				CREATE TABLE complex_pk_main_table
				(
					id1  INT,
					id2  INT,
					name VARCHAR(255) NOT NULL,
					PRIMARY KEY (id1, id2)
				);
			`,
			`
				CREATE TABLE complex_pk_ref_table_not_nullable
				(
					id    INT PRIMARY KEY,
					complex_pk_main_table_id1 INT NOT NULL,
					complex_pk_main_table_id2 INT NOT NULL,
					FOREIGN KEY (
						complex_pk_main_table_id1,
						complex_pk_main_table_id2
					) REFERENCES complex_pk_main_table (id1, id2)
				);
		`,
		}

		migrationDown := []string{
			`DROP TABLE complex_pk_ref_table_not_nullable;`,
			`DROP TABLE complex_pk_main_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		actual, err := i.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "complex_pk_ref_table_not_nullable")
		s.Require().NoError(err)
		expected := []models.Reference{
			{
				ReferencedSchema: "testdb",
				ReferencedName:   "complex_pk_main_table",
				ConstraintSchema: "testdb",
				ConstraintName:   "complex_pk_ref_table_not_nullable_ibfk_1",
				IsNullable:       false,
			},
		}
		diff := cmp.Diff(expected, actual)
		if diff != "" {
			s.T().Errorf("mismatch (-expected +actual):\n%s", diff)
		}
	})

	s.Run("two column fk nullable", func() {
		migrationUp := []string{
			`
				CREATE TABLE complex_pk_main_table
				(
					id1  INT,
					id2  INT,
					name VARCHAR(255) NOT NULL,
					PRIMARY KEY (id1, id2)
				);
			`,
			`
				CREATE TABLE complex_pk_ref_table_nullable
				(
					id    INT PRIMARY KEY,
					complex_pk_main_table_id1 INT,
					complex_pk_main_table_id2 INT,
					FOREIGN KEY (
						complex_pk_main_table_id1,
						complex_pk_main_table_id2
					) REFERENCES complex_pk_main_table (id1, id2)
				);
		`,
		}

		migrationDown := []string{
			`DROP TABLE complex_pk_ref_table_nullable;`,
			`DROP TABLE complex_pk_main_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		actual, err := i.getForeignKeyConstraints(ctx, &txMock{tx}, "testdb", "complex_pk_ref_table_nullable")
		s.Require().NoError(err)
		expected := []models.Reference{
			{
				ReferencedSchema: "testdb",
				ReferencedName:   "complex_pk_main_table",
				ConstraintSchema: "testdb",
				ConstraintName:   "complex_pk_ref_table_nullable_ibfk_1",
				IsNullable:       true,
			},
		}
		s.Require().Equal(expected, actual)
	})
}

func (s *mysqlSuite) TestIntrospector_getForeignKeyKeys() {
	ctx := context.Background()
	_, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
	s.Require().NoError(err)

	s.Run("unknown fk", func() {
		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		_, err = i.getForeignKeyKeys(ctx, &txMock{tx}, "testdb", "unknown_table_ibfk_1")
		s.Require().ErrorIs(err, errNoKeysFound)
	})

	s.Run("one fk column", func() {
		migrationUp := []string{
			`
			CREATE TABLE simple_main_table
			(
				id   INT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			);
		`,
			`
			CREATE TABLE simple_ref_table_not_nullable
			(
				id   					INT PRIMARY KEY,
				simple_main_table_id 	INT NOT NULL ,
				FOREIGN KEY (simple_main_table_id) REFERENCES simple_main_table (id)
			);
		`,
		}

		migrationDown := []string{
			`DROP TABLE simple_ref_table_not_nullable;`,
			`DROP TABLE simple_main_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		keys, err := i.getForeignKeyKeys(
			ctx,
			&txMock{tx},
			"testdb",
			"simple_ref_table_not_nullable_ibfk_1",
		)
		s.Require().NoError(err)
		s.Require().Equal([]string{"simple_main_table_id"}, keys)
	})

	s.Run("two fk columns", func() {
		migrationUp := []string{
			`
				CREATE TABLE complex_pk_main_table
				(
					id1  INT,
					id2  INT,
					name VARCHAR(255) NOT NULL,
					PRIMARY KEY (id1, id2)
				);
			`,
			`
				CREATE TABLE complex_pk_ref_table_nullable
				(
					id    INT PRIMARY KEY,
					complex_pk_main_table_id1 INT,
					complex_pk_main_table_id2 INT,
					FOREIGN KEY (
						complex_pk_main_table_id1,
						complex_pk_main_table_id2
					) REFERENCES complex_pk_main_table (id1, id2)
				);
		`,
		}

		migrationDown := []string{
			`DROP TABLE complex_pk_ref_table_nullable;`,
			`DROP TABLE complex_pk_main_table;`,
		}

		s.MigrateUp(ctx, migrationUp)
		defer s.MigrateDown(ctx, migrationDown)

		db, err := s.GetConnectionWithUser(ctx, mysqlRootUser, mysqlRootPass)
		s.Require().NoError(err)
		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i, err := NewIntrospector(newDefaultOptMock())
		s.Require().NoError(err)
		keys, err := i.getForeignKeyKeys(
			ctx,
			&txMock{tx},
			"testdb",
			"complex_pk_ref_table_nullable_ibfk_1",
		)
		s.Require().NoError(err)
		s.Require().Equal([]string{"complex_pk_main_table_id1", "complex_pk_main_table_id2"}, keys)
	})
}

func compareTables(t *testing.T, expected, actual []models2.Table) {
	t.Helper()
	require.Lenf(t, actual, len(expected), "expected %d tables, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equal(t, expected[i].Name, actual[i].Name)
		assert.Equal(t, expected[i].Schema, actual[i].Schema)
		compareColumns(t, expected[i].Columns, actual[i].Columns)
	}
}

func compareColumns(t *testing.T, expected, actual []models2.Column) {
	t.Helper()
	require.Lenf(t, actual, len(expected), "expected %d columns, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equal(t, expected[i].Name, actual[i].Name)
		assert.Equal(t, expected[i].TypeName, actual[i].TypeName)
		assert.Equal(t, expected[i].NotNull, actual[i].NotNull)
		assert.Equal(t, expected[i].DataType != nil, actual[i].DataType != nil)
		if expected[i].DataType != nil && actual[i].DataType != nil {
			assert.Equal(t, *expected[i].DataType, *actual[i].DataType)
		}
		assert.Equal(t, expected[i].NumericPrecision != nil, actual[i].NumericPrecision != nil)
		if expected[i].NumericPrecision != nil && actual[i].NumericPrecision != nil {
			assert.Equal(t, *expected[i].NumericPrecision, *actual[i].NumericPrecision)
		}
		assert.Equal(t, expected[i].NumericScale != nil, actual[i].NumericScale != nil)
		if expected[i].NumericScale != nil && actual[i].NumericScale != nil {
			assert.Equal(t, *expected[i].NumericScale, *actual[i].NumericScale)
		}
		assert.Equal(t, expected[i].DateTimePrecision != nil, actual[i].DateTimePrecision != nil)
		if expected[i].DateTimePrecision != nil && actual[i].DateTimePrecision != nil {
			assert.Equal(t, *expected[i].DateTimePrecision, *actual[i].DateTimePrecision)
		}
	}
}

func newPtr[T any](v T) *T {
	return &v
}
