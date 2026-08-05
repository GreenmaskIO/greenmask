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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
)

const (
	seqPrivilegesLastValue    = 100
	seqPrivilegesSequenceName = "seq_priv_table_id_seq"
)

// SequencePrivilegesSuite - regression suite for a dump role that has SELECT on tables but no
// privilege on the sequences behind them. Since PostgreSQL 18 pg_sequence_last_value returns NULL
// instead of raising, so greenmask must detect the missing privilege itself.
type SequencePrivilegesSuite struct {
	suite.Suite
	tmpDir        string
	storageDir    string
	conn          *pgx.Conn
	dbConfig      *pgx.ConnConfig
	sourceDbName  string
	restoreDbName string
	dumperUser    string
	dumperPass    string
}

func (suite *SequencePrivilegesSuite) SetupSuite() {
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	ctx := context.Background()

	var err error
	suite.dbConfig, err = pgx.ParseConfig(uri)
	suite.Require().NoError(err)

	suite.tmpDir, err = os.MkdirTemp(tempDir, "sequence_privileges_test_")
	suite.Require().NoError(err)

	suite.storageDir = path.Join(suite.tmpDir, "storage")
	suite.Require().NoError(os.Mkdir(suite.storageDir, 0700))

	suite.conn, err = pgx.ConnectConfig(ctx, suite.dbConfig)
	suite.Require().NoError(err)

	suffix := time.Now().UnixMilli()
	suite.sourceDbName = fmt.Sprintf("seq_priv_source_%d", suffix)
	suite.restoreDbName = fmt.Sprintf("seq_priv_restore_%d", suffix)
	suite.dumperUser = fmt.Sprintf("seq_priv_dumper_%d", suffix)
	suite.dumperPass = "seq_priv_dumper_password"

	_, err = suite.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", suite.sourceDbName))
	suite.Require().NoError(err)

	// NOINHERIT keeps the role from silently picking up the table owner's privileges
	_, err = suite.conn.Exec(ctx, fmt.Sprintf(
		"CREATE ROLE %s LOGIN NOINHERIT PASSWORD '%s'", suite.dumperUser, suite.dumperPass,
	))
	suite.Require().NoError(err)

	_, err = suite.conn.Exec(ctx, fmt.Sprintf(
		"GRANT CONNECT ON DATABASE %s TO %s", suite.sourceDbName, suite.dumperUser,
	))
	suite.Require().NoError(err)

	sourceConn := suite.connectTo(ctx, suite.sourceDbName)
	defer sourceConn.Close(ctx) // nolint: errcheck

	_, err = sourceConn.Exec(ctx, `
		CREATE TABLE seq_priv_table
		(
			id BIGSERIAL PRIMARY KEY,
			v  TEXT
		);
		INSERT INTO seq_priv_table (v) SELECT 'x' FROM generate_series(1, 100);
	`)
	suite.Require().NoError(err)

	// The readonly role setup from the issue report: everything needed to read the table data and
	// nothing at all on the sequence behind the bigserial column.
	_, err = sourceConn.Exec(ctx, fmt.Sprintf(`
		GRANT USAGE ON SCHEMA public TO %[1]s;
		GRANT SELECT ON ALL TABLES IN SCHEMA public TO %[1]s;
	`, suite.dumperUser))
	suite.Require().NoError(err)
}

func (suite *SequencePrivilegesSuite) connectTo(ctx context.Context, dbName string) *pgx.Conn {
	cfg := suite.dbConfig.Copy()
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	suite.Require().NoError(err)
	return conn
}

// runGreenmaskAs - runs the greenmask binary against dbName as the given role and returns its
// combined stdout and stderr so that the reported diagnostic can be asserted on.
func (suite *SequencePrivilegesSuite) runGreenmaskAs(
	user, password, dbName string, args ...string,
) (string, error) {
	greenmaskBin := path.Join(greenmaskBinPath, "greenmask")
	cmd := exec.Command(greenmaskBin, args...)

	env := append(os.Environ(),
		fmt.Sprintf("PATH=%s:%s", pgBinPath, os.Getenv("PATH")),
		fmt.Sprintf("PGDATABASE=%s", dbName),
		fmt.Sprintf("PGHOST=%s", suite.dbConfig.Host),
		fmt.Sprintf("PGPORT=%d", suite.dbConfig.Port),
		fmt.Sprintf("PGUSER=%s", user),
		fmt.Sprintf("PGPASSWORD=%s", password),
		fmt.Sprintf("STORAGE_TYPE=%s", "directory"),
		fmt.Sprintf("STORAGE_DIRECTORY_PATH=%s", suite.storageDir),
		fmt.Sprintf("COMMON_PG_BIN_PATH=%s", pgBinPath),
		fmt.Sprintf("COMMON_TMP_DIR=%s", suite.tmpDir),
	)
	if sslMode, ok := suite.dbConfig.RuntimeParams["sslmode"]; ok {
		env = append(env, fmt.Sprintf("PGSSLMODE=%s", sslMode))
	} else {
		env = append(env, "PGSSLMODE=disable")
	}
	cmd.Env = env

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	err := cmd.Run()
	fmt.Printf("GREENMASK %v OUTPUT:\n%s\n", args, output.String())
	return output.String(), err
}

func (suite *SequencePrivilegesSuite) TestSequencePrivileges() {
	ctx := context.Background()

	// Sub-steps share state and must run in order, so they live in a single test method.
	suite.Run("dump fails when the sequence cannot be read", func() {
		output, err := suite.runGreenmaskAs(
			suite.dumperUser, suite.dumperPass, suite.sourceDbName, "dump",
		)
		suite.Require().Error(err, "dump must not succeed without sequence privileges")
		suite.Assert().Contains(output, "permission denied for sequence")
		suite.Assert().Contains(output, seqPrivilegesSequenceName)
	})

	suite.Run("validate reports the same problem", func() {
		output, err := suite.runGreenmaskAs(
			suite.dumperUser, suite.dumperPass, suite.sourceDbName, "validate",
		)
		suite.Require().Error(err, "validate must not succeed without sequence privileges")
		suite.Assert().Contains(output, "permission denied for sequence")
		suite.Assert().Contains(output, seqPrivilegesSequenceName)
	})

	suite.Run("dump succeeds once the privilege is granted", func() {
		sourceConn := suite.connectTo(ctx, suite.sourceDbName)
		defer sourceConn.Close(ctx) // nolint: errcheck
		_, err := sourceConn.Exec(ctx, fmt.Sprintf(
			"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO %s", suite.dumperUser,
		))
		suite.Require().NoError(err)

		_, err = suite.runGreenmaskAs(
			suite.dumperUser, suite.dumperPass, suite.sourceDbName, "dump",
		)
		suite.Require().NoError(err, "dump failed")
	})

	suite.Run("restore preserves the sequence last value", func() {
		_, err := suite.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", suite.restoreDbName))
		suite.Require().NoError(err)

		// "latest" only considers dumps that produced a metadata file, so the failed dump from the
		// first sub-step cannot be picked up here.
		_, err = suite.runGreenmaskAs(
			suite.dbConfig.User, suite.dbConfig.Password, suite.restoreDbName,
			"restore", "latest",
		)
		suite.Require().NoError(err, "restore failed")

		targetConn := suite.connectTo(ctx, suite.restoreDbName)
		defer targetConn.Close(ctx) // nolint: errcheck

		var lastValue int64
		var isCalled bool
		err = targetConn.QueryRow(ctx,
			fmt.Sprintf("SELECT last_value, is_called FROM %s", seqPrivilegesSequenceName),
		).Scan(&lastValue, &isCalled)
		suite.Require().NoError(err)
		suite.Assert().EqualValues(seqPrivilegesLastValue, lastValue)
		suite.Assert().True(isCalled)
	})
}

func (suite *SequencePrivilegesSuite) TearDownSuite() {
	ctx := context.Background()
	if suite.conn != nil {
		for _, dbName := range []string{suite.sourceDbName, suite.restoreDbName} {
			if dbName == "" {
				continue
			}
			suite.conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName)) // nolint: errcheck
		}
		if suite.dumperUser != "" {
			suite.conn.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", suite.dumperUser)) // nolint: errcheck
		}
		suite.conn.Close(ctx) // nolint: errcheck
	}
	if suite.tmpDir != "" {
		os.RemoveAll(suite.tmpDir) // nolint: errcheck
	}
}
