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

package restorers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

type dummyTaskMapper struct{}

func (*dummyTaskMapper) SetTaskCompleted(_ commonmodels.TaskID) {
	// no-op
}

func (*dummyTaskMapper) IsTaskCompleted(_ commonmodels.TaskID) bool {
	return true
}

type restoreSuite struct {
	testutils.MySQLContainerSuite
}

func (s *restoreSuite) TestMySQLContainerSuite() {
	s.Require().NotNil(s.Container)
}

func TestMySQL(t *testing.T) {
	suite.Run(t, new(restoreSuite))
}

func (s *restoreSuite) SetupSuite() {
	s.MySQLContainerSuite.SetMigrationUp([]string{
		`CREATE DATABASE IF NOT EXISTS playground;`,
		`USE playground;`,
		`CREATE TABLE playground.users
		(
			id         INT AUTO_INCREMENT PRIMARY KEY,
			username   VARCHAR(50)  NOT NULL,
			email      VARCHAR(100) NOT NULL UNIQUE,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE playground.orders
		(
			id         INT AUTO_INCREMENT PRIMARY KEY,
			user_id    INT            NOT NULL,
			product    VARCHAR(100)   NOT NULL,
			amount     DECIMAL(10, 2) NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users (id)
		);`,
	})

	cwd, err := os.Getwd()
	s.Require().NoError(err)
	settingsFile := path.Join(cwd, "testdata", "settings.sql")
	s.MySQLContainerSuite.SetScripts(settingsFile)

	s.MySQLContainerSuite.SetupSuite()

	db, err := s.GetRootConnection(context.Background())
	s.Require().NoError(err)
	defer db.Close()
	_, err = db.Exec("SET GLOBAL local_infile = 1;")
	s.Require().NoError(err, "failed to enable local infile")

	_, err = db.Exec("SET GLOBAL general_log = 'ON';")
	s.Require().NoError(err, "failed to enable local infile")

	_, err = db.Exec("SET GLOBAL log_output = 'TABLE'; -- or 'FILE'")
	s.Require().NoError(err, "failed to enable local infile")
}

func (s *restoreSuite) TestRestorer_RestoreData() {
	err := utils.SetDefaultContextLogger(zerolog.LevelDebugValue, utils.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	r, err := os.Open("testdata/playground__users.csv")
	s.Require().NoError(err)
	defer r.Close()

	table := commonmodels.Table{
		Schema: "playground",
		Name:   "users",
	}
	rawData := utils.Must(json.Marshal(table))
	meta := commonmodels.RestorationItem{
		Filename:         "playground__users.csv.gz",
		RecordCount:      3,
		ObjectDefinition: rawData,
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, meta.Filename).
		Return(r, nil)

	opts := s.GetRootConnectionOpts(ctx)
	tr := &dummyTaskMapper{}
	rr, err := NewTableDataRestorer(meta, opts, st, tr)
	assert.NoError(s.T(), err)
	err = rr.Init(ctx)
	s.Require().NoError(err, "failed to open table data restorer")
	err = rr.Restore(ctx)
	s.Require().NoError(err)

	db, err := s.GetRootConnection(context.Background())
	rows, err := db.Query("SELECT * FROM playground.users")
	s.Require().NoError(err, "failed to query restored data")
	var actual [][]string
	for rows.Next() {
		var col1, col2, col3, col4 string
		err = rows.Scan(&col1, &col2, &col3, &col4)
		s.Require().NoError(err, "failed to scan row")
		col4 = strings.Replace(col4, "Z", "", 1)
		col4 = strings.Replace(col4, "T", " ", 1)
		actual = append(actual, []string{col1, col2, col3, col4})
	}

	originalData, err := os.Open("testdata/playground__users.csv")
	s.Require().NoError(err)
	defer originalData.Close()
	expected, err := csv.NewReader(originalData).ReadAll()
	s.Require().NoError(err, "failed to read expected data")
	s.Require().Equal(expected, actual, "restored data does not match expected data")
}
