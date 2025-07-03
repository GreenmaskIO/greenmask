package testutils

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

var (
	migrationUp = `
CREATE TABLE test_table (
    	id INT PRIMARY KEY AUTO_INCREMENT,
    	name VARCHAR(255) NOT NULL
);
	`
	migrationDown = `
DROP TABLE test_table;
	`
)

type mysqlTestSuite struct {
	MySQLContainerSuite
}

func (s *mysqlTestSuite) SetupSuite() {
	s.SetMigrationUp(migrationUp).
		SetMigrationDown(migrationDown).
		SetupSuite()
}

func (s *mysqlTestSuite) TearDownSuite() {
	s.MySQLContainerSuite.TearDownSuite()
}

func (s *mysqlTestSuite) TestMySQLContainerSuite() {
	s.Require().NotNil(s.Container)
}

func TestRestorers(t *testing.T) {
	suite.Run(t, new(mysqlTestSuite))
}
