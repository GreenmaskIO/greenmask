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
	"encoding/json"
	"io"
	"strings"

	"github.com/greenmaskio/greenmask/pkg/common/mocks"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	utils2 "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
)

func (s *restoreSuite) TestRestorerInsert_RestoreData() {
	err := utils2.SetDefaultContextLogger(zerolog.LevelDebugValue, utils2.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	db, err := s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database before test")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err, "failed to disable foreign key checks")
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err, "failed to truncate table")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err, "failed to enable foreign key checks")
	db.Close()

	sqlContent := `
		INSERT INTO playground.users (username, email) VALUES 
		    ('insertUser1', 'insertUser1@example.com'), 
		    ('insertUser2', 'insertUser2@example.com');
	`
	r := strings.NewReader(sqlContent)

	table := models.Table{
		Schema: "playground",
		Name:   "users",
	}
	rawData := utils2.Must(json.Marshal(table))
	meta := models.RestorationItem{
		Filename:         "playground__users.sql",
		RecordCount:      2,
		ObjectDefinition: rawData,
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, meta.Filename).
		Return(io.NopCloser(r), nil)

	opts := s.GetRootConnectionOpts(ctx)
	tr := &dummyTaskMapper{}
	rr, err := NewTableDataRestorerInsert(meta, opts, st, tr)
	s.NoError(err)
	err = rr.Init(ctx)
	s.Require().NoError(err, "failed to init table data restorer")
	err = rr.Restore(ctx)
	s.Require().NoError(err)
	err = rr.Close(ctx)
	s.Require().NoError(err)

	db, err = s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database")
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM playground.users WHERE username LIKE 'insertUser%'").Scan(&count)
	s.Require().NoError(err, "failed to query restored data")
	s.Require().Equal(2, count, "restored data count does not match")

	// Final cleanup
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err)
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err)
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err)
}

func (s *restoreSuite) TestRestorerInsert_RestoreData_Batched() {
	err := utils2.SetDefaultContextLogger(zerolog.LevelDebugValue, utils2.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	db, err := s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database before test")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err, "failed to disable foreign key checks")
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err, "failed to truncate table")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err, "failed to enable foreign key checks")
	db.Close()

	sqlContent := `
		INSERT INTO playground.users (username, email) VALUES 
		    ('insertUser1', 'insertUser1@example.com'), 
		    ('insertUser2', 'insertUser2@example.com');
		INSERT INTO playground.users (username, email) VALUES 
		    ('insertUser3', 'insertUser3@example.com'), 
		    ('insertUser4', 'insertUser4@example.com');
		INSERT INTO playground.users (username, email) VALUES 
		    ('insertUser5', 'insertUser5@example.com'), 
		    ('insertUser6', 'insertUser6@example.com');
	`
	r := strings.NewReader(sqlContent)

	table := models.Table{
		Schema: "playground",
		Name:   "users",
	}
	rawData := utils2.Must(json.Marshal(table))
	meta := models.RestorationItem{
		Filename:         "playground__users.sql",
		RecordCount:      6,
		ObjectDefinition: rawData,
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, meta.Filename).
		Return(io.NopCloser(r), nil)

	opts := s.GetRootConnectionOpts(ctx)
	tr := &dummyTaskMapper{}
	rr, err := NewTableDataRestorerInsert(meta, opts, st, tr)
	s.NoError(err)
	err = rr.Init(ctx)
	s.Require().NoError(err, "failed to init table data restorer")
	err = rr.Restore(ctx)
	s.Require().NoError(err)
	err = rr.Close(ctx)
	s.Require().NoError(err)

	db, err = s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database")
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM playground.users WHERE username LIKE 'insertUser%'").Scan(&count)
	s.Require().NoError(err, "failed to query restored data")
	s.Require().Equal(6, count, "restored data count does not match")

	// Final cleanup
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err)
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err)
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err)
}

func (s *restoreSuite) TestRestorerInsert_RestoreData_SingleRow() {
	err := utils2.SetDefaultContextLogger(zerolog.LevelDebugValue, utils2.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	db, err := s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database before test")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err, "failed to disable foreign key checks")
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err, "failed to truncate table")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err, "failed to enable foreign key checks")
	db.Close()

	sqlContent := `
		INSERT INTO playground.users (username, email) VALUES ('insertUser1', 'insertUser1@example.com');
		INSERT INTO playground.users (username, email) VALUES ('insertUser2', 'insertUser2@example.com');
		INSERT INTO playground.users (username, email) VALUES ('insertUser3', 'insertUser3@example.com');
		INSERT INTO playground.users (username, email) VALUES ('insertUser4', 'insertUser4@example.com');
		INSERT INTO playground.users (username, email) VALUES ('insertUser5', 'insertUser5@example.com');
		INSERT INTO playground.users (username, email) VALUES ('insertUser6', 'insertUser6@example.com');
	`
	r := strings.NewReader(sqlContent)

	table := models.Table{
		Schema: "playground",
		Name:   "users",
	}
	rawData := utils2.Must(json.Marshal(table))
	meta := models.RestorationItem{
		Filename:         "playground__users.sql",
		RecordCount:      6,
		ObjectDefinition: rawData,
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, meta.Filename).
		Return(io.NopCloser(r), nil)

	opts := s.GetRootConnectionOpts(ctx)
	tr := &dummyTaskMapper{}
	rr, err := NewTableDataRestorerInsert(meta, opts, st, tr)
	s.NoError(err)
	err = rr.Init(ctx)
	s.Require().NoError(err, "failed to init table data restorer")
	err = rr.Restore(ctx)
	s.Require().NoError(err)
	err = rr.Close(ctx)
	s.Require().NoError(err)

	db, err = s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database")
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM playground.users WHERE username LIKE 'insertUser%'").Scan(&count)
	s.Require().NoError(err, "failed to query restored data")
	s.Require().Equal(6, count, "restored data count does not match")

	// Final cleanup
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err)
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err)
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err)
}
func (s *restoreSuite) TestRestorerInsert_RestoreData_CRLF_TrailingSpaces() {
	err := utils2.SetDefaultContextLogger(zerolog.LevelDebugValue, utils2.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	db, err := s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database before test")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err, "failed to disable foreign key checks")
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err, "failed to truncate table")
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err, "failed to enable foreign key checks")
	db.Close()

	// Use CRLF (\r\n) and trailing spaces before semicolon
	sqlContent := "INSERT INTO playground.users (username, email) VALUES \r\n" +
		"    ('insertUser1', 'insertUser1@example.com'), \r\n" +
		"    ('insertUser2', 'insertUser2@example.com')  ;   \r\n" +
		"INSERT INTO playground.users (username, email) VALUES \r\n" +
		"    ('insertUser3', 'insertUser3@example.com');\r\n"
	r := strings.NewReader(sqlContent)

	table := models.Table{
		Schema: "playground",
		Name:   "users",
	}
	rawData := utils2.Must(json.Marshal(table))
	meta := models.RestorationItem{
		Filename:         "playground__users.sql",
		RecordCount:      3,
		ObjectDefinition: rawData,
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, meta.Filename).
		Return(io.NopCloser(r), nil)

	opts := s.GetRootConnectionOpts(ctx)
	tr := &dummyTaskMapper{}
	rr, err := NewTableDataRestorerInsert(meta, opts, st, tr)
	s.NoError(err)
	err = rr.Init(ctx)
	s.Require().NoError(err, "failed to init table data restorer")
	err = rr.Restore(ctx)
	s.Require().NoError(err)
	err = rr.Close(ctx)
	s.Require().NoError(err)

	db, err = s.GetRootConnection(context.Background())
	s.Require().NoError(err, "failed to connect to database")
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM playground.users WHERE username LIKE 'insertUser%'").Scan(&count)
	s.Require().NoError(err, "failed to query restored data")
	s.Require().Equal(3, count, "restored data count does not match")

	// Final cleanup
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	s.Require().NoError(err)
	_, err = db.Exec("TRUNCATE TABLE playground.users")
	s.Require().NoError(err)
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	s.Require().NoError(err)
}
