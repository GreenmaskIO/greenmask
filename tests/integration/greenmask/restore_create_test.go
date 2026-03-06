// Copyright 2023 Greenmask
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

package greenmask

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
)

type RestoreCreateSuite struct {
	suite.Suite
	tmpDir       string
	storageDir   string
	conn         *pgx.Conn
	sourceDbName string
	dbConfig     *pgx.ConnConfig
}

func (suite *RestoreCreateSuite) SetupSuite() {
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	var err error
	suite.dbConfig, err = pgx.ParseConfig(uri)
	suite.Require().NoError(err)

	suite.tmpDir, err = os.MkdirTemp(tempDir, "restore_create_test_")
	suite.Require().NoError(err)

	suite.storageDir = path.Join(suite.tmpDir, "storage")
	err = os.Mkdir(suite.storageDir, 0700)
	suite.Require().NoError(err)

	suite.conn, err = pgx.ConnectConfig(context.Background(), suite.dbConfig)
	suite.Require().NoError(err)

	// Create source DB with special properties
	suite.sourceDbName = fmt.Sprintf("rc_source_%d", time.Now().UnixMilli())
	_, err = suite.conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", suite.sourceDbName))
	suite.Require().NoError(err)

	sourceConfig := suite.dbConfig.Copy()
	sourceConfig.Database = suite.sourceDbName
	sourceConn, err := pgx.ConnectConfig(context.Background(), sourceConfig)
	suite.Require().NoError(err)
	defer sourceConn.Close(context.Background())

	// Set database-level properties
	_, err = sourceConn.Exec(context.Background(), fmt.Sprintf("COMMENT ON DATABASE %s IS 'test comment'", suite.sourceDbName))
	suite.Require().NoError(err)
	_, err = sourceConn.Exec(context.Background(), fmt.Sprintf("ALTER DATABASE %s SET search_path = public, custom_schema", suite.sourceDbName))
	suite.Require().NoError(err)
	_, err = sourceConn.Exec(context.Background(), fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC", suite.sourceDbName))
	suite.Require().NoError(err)
}

func (suite *RestoreCreateSuite) runGreenmask(dbName string, args ...string) error {
	greenmaskBin := path.Join(greenmaskBinPath, "greenmask")
	cmd := exec.Command(greenmaskBin, args...)

	// Prepare environment based on parsed DB config
	env := append(os.Environ(),
		fmt.Sprintf("PATH=%s:%s", pgBinPath, os.Getenv("PATH")),
		fmt.Sprintf("PGDATABASE=%s", dbName),
		fmt.Sprintf("PGHOST=%s", suite.dbConfig.Host),
		fmt.Sprintf("PGPORT=%d", suite.dbConfig.Port),
		fmt.Sprintf("PGUSER=%s", suite.dbConfig.User),
		fmt.Sprintf("PGPASSWORD=%s", suite.dbConfig.Password),
		fmt.Sprintf("STORAGE_TYPE=%s", "directory"),
		fmt.Sprintf("STORAGE_DIRECTORY_PATH=%s", suite.storageDir),
		fmt.Sprintf("COMMON_PG_BIN_PATH=%s", pgBinPath),
		fmt.Sprintf("COMMON_TMP_DIR=%s", suite.tmpDir),
	)

	// Handle SSL mode if present in connection string
	if sslMode, ok := suite.dbConfig.RuntimeParams["sslmode"]; ok {
		env = append(env, fmt.Sprintf("PGSSLMODE=%s", sslMode))
	} else {
		env = append(env, "PGSSLMODE=disable")
	}

	cmd.Env = env

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fmt.Printf("STDOUT: %s\n", scanner.Text())
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Printf("STDERR: %s\n", scanner.Text())
		}
	}()

	return cmd.Wait()
}

func (suite *RestoreCreateSuite) TestRestoreCreate() {
	ctx := context.Background()

	suite.Run("dumping data", func() {
		err := suite.runGreenmask(suite.sourceDbName, "dump")
		suite.Require().NoError(err, "dump failed")
	})

	suite.Run("dropping source db", func() {
		_, err := suite.conn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s", suite.sourceDbName))
		suite.Require().NoError(err)
	})

	suite.Run("restoring with --create", func() {
		entry, err := os.ReadDir(suite.storageDir)
		suite.Require().NoError(err)
		suite.Require().NotEmpty(entry, "no dumps found in storage")
		lastDump := entry[0]

		// Use the maintenance database from original config (typically "postgres")
		err = suite.runGreenmask(suite.dbConfig.Database, "restore",
			"--create",
			lastDump.Name(),
		)
		suite.Require().NoError(err, "restore failed")
	})

	suite.Run("verifying properties", func() {
		// Connect to the recreated database
		verifyConfig := suite.dbConfig.Copy()
		verifyConfig.Database = suite.sourceDbName
		targetConn, err := pgx.ConnectConfig(ctx, verifyConfig)
		suite.Require().NoError(err)
		defer targetConn.Close(ctx)

		// Verify comment
		var comment string
		err = targetConn.QueryRow(ctx, "SELECT description FROM pg_shdescription WHERE objoid = (SELECT oid FROM pg_database WHERE datname = current_database())").Scan(&comment)
		suite.Require().NoError(err)
		suite.Assert().Equal("test comment", comment)

		// Verify ALTER DATABASE SET property
		var searchPath string
		err = targetConn.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name = 'search_path'").Scan(&searchPath)
		suite.Require().NoError(err)
		suite.Assert().Contains(searchPath, "custom_schema")
	})
}

func (suite *RestoreCreateSuite) TearDownSuite() {
	if suite.conn != nil {
		if suite.sourceDbName != "" {
			suite.conn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", suite.sourceDbName))
		}
		suite.conn.Close(context.Background())
	}
	if suite.tmpDir != "" {
		os.RemoveAll(suite.tmpDir)
	}
}

func TestRestoreCreate(t *testing.T) {
	suite.Run(t, new(RestoreCreateSuite))
}
