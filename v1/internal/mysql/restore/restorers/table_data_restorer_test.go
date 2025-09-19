package restorers

import (
	"context"
	"encoding/csv"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

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
	meta := commonmodels.DataSectionEntry{
		FileName:    "playground__users.csv.gz",
		RecordCount: 3,
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, meta.FileName).
		Return(r, nil)

	opts := s.GetRootConnectionOpts(ctx)
	rr := NewTableDataRestorer(table, meta, opts, st)
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
