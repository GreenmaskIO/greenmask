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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	mysqlmodels "github.com/greenmaskio/greenmask/v1/internal/mysql/models"
	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

const (
	mysqlRootUser = testutils.MysqlRootUser
	mysqlRootPass = testutils.MysqlRootPassword
)

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
		opt.On("GetExcludedSchemas").Return(nil)
		opt.On("GetIncludedSchemas").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i := NewIntrospector(opt)
		err = i.Introspect(ctx, tx)
		s.Require().NoError(err)

		expected := []mysqlmodels.Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []mysqlmodels.Column{
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
				Columns: []mysqlmodels.Column{
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
				Columns: []mysqlmodels.Column{
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
		opt.On("GetExcludedSchemas").Return(nil)
		opt.On("GetIncludedSchemas").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i := NewIntrospector(opt)
		err = i.Introspect(ctx, tx)
		s.Require().NoError(err)

		expected := []mysqlmodels.Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []mysqlmodels.Column{
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
		opt.On("GetExcludedSchemas").Return(nil)
		opt.On("GetIncludedSchemas").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i := NewIntrospector(opt)
		err = i.Introspect(ctx, tx)
		s.Require().NoError(err)

		expected := []mysqlmodels.Table{
			{
				Name:   "test_table_2",
				Schema: "testdb",
				Columns: []mysqlmodels.Column{
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
				Columns: []mysqlmodels.Column{
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
		opt.On("GetExcludedSchemas").Return([]string{"testdb"})
		opt.On("GetIncludedSchemas").Return(nil)

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i := NewIntrospector(opt)
		err = i.Introspect(ctx, tx)
		s.Require().NoError(err)

		expected := []mysqlmodels.Table{
			{
				Name:   "test_table_3",
				Schema: "testdb1",
				Columns: []mysqlmodels.Column{
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
		opt.On("GetExcludedSchemas").Return(nil)
		opt.On("GetIncludedSchemas").Return([]string{"testdb1"})

		tx, err := db.Begin()
		s.NoError(err)
		defer func() {
			_ = tx.Rollback()
		}()
		i := NewIntrospector(opt)
		err = i.Introspect(ctx, tx)
		s.Require().NoError(err)

		expected := []mysqlmodels.Table{
			{
				Name:   "test_table_3",
				Schema: "testdb1",
				Columns: []mysqlmodels.Column{
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
		i := NewIntrospector(&optMock{})
		pks, err := i.getPrimaryKey(ctx, tx, "testdb", "simple_table")
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
		i := NewIntrospector(&optMock{})
		pks, err := i.getPrimaryKey(ctx, tx, "testdb", "simple_table_with_no_pk")
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
		i := NewIntrospector(&optMock{})
		refs, err := i.getForeignKeyConstraints(ctx, tx, "testdb", "simple_table_with_no_fk")
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
		i := NewIntrospector(&optMock{})
		actual, err := i.getForeignKeyConstraints(ctx, tx, "testdb", "simple_ref_table_not_nullable")
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
		i := NewIntrospector(&optMock{})
		actual, err := i.getForeignKeyConstraints(ctx, tx, "testdb", "simple_ref_table_nullable")
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
		i := NewIntrospector(&optMock{})
		actual, err := i.getForeignKeyConstraints(ctx, tx, "testdb", "complex_pk_ref_table_not_nullable")
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
		i := NewIntrospector(&optMock{})
		actual, err := i.getForeignKeyConstraints(ctx, tx, "testdb", "complex_pk_ref_table_nullable")
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
		i := NewIntrospector(&optMock{})
		_, err = i.getForeignKeyKeys(ctx, tx, "testdb", "unknown_table_ibfk_1")
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
		i := NewIntrospector(&optMock{})
		keys, err := i.getForeignKeyKeys(
			ctx,
			tx,
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
		i := NewIntrospector(&optMock{})
		keys, err := i.getForeignKeyKeys(
			ctx,
			tx,
			"testdb",
			"complex_pk_ref_table_nullable_ibfk_1",
		)
		s.Require().NoError(err)
		s.Require().Equal([]string{"complex_pk_main_table_id1", "complex_pk_main_table_id2"}, keys)
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
