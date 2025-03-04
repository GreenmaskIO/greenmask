package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v2/internal/testutils"
)

var (
	migrationUp = `
CREATE TABLE test_table (
    	id INT PRIMARY KEY AUTO_INCREMENT,
    	name VARCHAR(255) NOT NULL
);
`
)

type mysqlSuite struct {
	testutils.MySQLContainerSuite
}

func (s *mysqlSuite) SetupSuite() {
	s.SetMigrationUp(migrationUp).
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
	db, err := s.GetConnection(ctx)
	s.Require().NoError(err)

	s.Run("basic", func() {
		i := NewIntrospector(db)
		err = i.Introspect()
		s.Require().NoError(err)

		s.Require().Len(i.tables, 1)
		table := i.tables[0]
		s.Require().Equal("test_table", table.Name)
		s.Require().Equal("testdb", table.Schema)
		s.Require().Len(table.Columns, 2)
		s.Require().Equal("id", table.Columns[0].Name)
		s.Require().Equal("name", table.Columns[1].Name)
	})
}
