package introspect

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

var (
	migrationUp = []string{
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
)

const (
	mysqlRootUser = "root"
	mysqlRootPass = "root"
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

func (s *mysqlSuite) SetupSuite() {
	s.SetMigrationUp(migrationUp).
		SetMigrationUser(mysqlRootUser, mysqlRootPass).
		SetRootUser(mysqlRootUser, mysqlRootPass).
		SetupSuite()
}

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

	s.Run("basic", func() {
		opt := &optMock{}
		opt.On("GetIncludedTables").Return(nil)
		opt.On("GetExcludedTables").Return(nil)
		opt.On("GetExcludedSchemas").Return(nil)
		opt.On("GetIncludedSchemas").Return(nil)

		i := NewIntrospector(db, opt)
		err = i.Introspect(ctx)
		s.Require().NoError(err)

		expected := []Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []Column{
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
				Columns: []Column{
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
				Columns: []Column{
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

		i := NewIntrospector(db, opt)
		err = i.Introspect(ctx)
		s.Require().NoError(err)

		expected := []Table{
			{
				Name:   "test_table_1",
				Schema: "testdb",
				Columns: []Column{
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

func compareTables(t *testing.T, expected, actual []Table) {
	t.Helper()
	require.Lenf(t, actual, len(expected), "expected %d tables, got %d", len(expected), len(actual))
	for i := range expected {
		assert.Equal(t, expected[i].Name, actual[i].Name)
		assert.Equal(t, expected[i].Schema, actual[i].Schema)
		compareColumns(t, expected[i].Columns, actual[i].Columns)
	}
}

func compareColumns(t *testing.T, expected, actual []Column) {
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
