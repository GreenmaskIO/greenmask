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
	s.SetMigrationUp([]string{migrationUp}).
		SetMigrationDown([]string{migrationDown}).
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
