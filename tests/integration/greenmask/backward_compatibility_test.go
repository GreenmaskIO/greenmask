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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
)

var configStr = template.Must(template.New("config").Parse(`
common:
  pg_bin_path: "{{ .pgBinPath }}"
  tmp_dir: "{{ .tmpDir }}"

log:
  level: debug
  format: json

storage:
  type: "directory"
  directory:
    path: "{{ .storageDir }}"

dump:
  pg_dump_options:
    dbname: "{{ .uri }}"
    jobs: 10
    load-via-partition-root: true
    schema: public
  
  transformation:
{{ if ge .version 120000 }}
    - schema: public
      name: "people"
      transformers:
        - name: "Masking"
          params:
            column: "first_name"
            type: "name"
{{ end }}

    - schema: "bookings"
      name: "flights"
      transformers:
        
        - name: "RandomDate"
          params:
            "min": "2023-01-01 00:00:00.0+03"
            "max": "2023-01-02 00:00:00.0+03"
            "column": "scheduled_departure"
        
        - name: "RandomDate"
          params:
            "min": "2023-02-02 01:00:00.0+03"
            "max": "2023-03-03 00:00:00.0+03"
            "column": "scheduled_arrival"
`))

type BackwardCompatibilitySuite struct {
	suite.Suite
	tmpDir            string
	runtimeTmpDir     string
	storageDir        string
	configFilePath    string
	conn              *pgx.Conn
	restorationDbName string
	pgVersionNum      int
}

func (suite *BackwardCompatibilitySuite) SetupSuite() {
	log.Debug().Msg("URI: " + uri)
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	// Creating tmp dir
	var err error
	suite.tmpDir, err = os.MkdirTemp(tempDir, "backward_compatibility_test_")
	suite.Require().NoError(err, "error creating temp directory")
	log.Debug().Str("dir", suite.tmpDir).Msg("created temp directory")

	// Creating directory for storage
	suite.storageDir = path.Join(suite.tmpDir, "storage")
	err = os.Mkdir(suite.storageDir, 0700)
	suite.Require().NoError(err, "error creating storage dir")

	// Creating directory for tmp
	suite.runtimeTmpDir = path.Join(suite.tmpDir, "tmp")
	err = os.Mkdir(suite.runtimeTmpDir, 0700)
	suite.Require().NoError(err, "error creating tmp dir")

	suite.conn, err = pgx.Connect(context.Background(), uri)
	suite.Require().NoError(err, "error connecting to db")

	// TODO: Delete db and create then
	suite.restorationDbName = fmt.Sprintf("demo_restore_%d", time.Now().UnixMilli())
	log.Info().Str("dbname", suite.restorationDbName).Msg("creating database")
	_, err = suite.conn.Exec(context.Background(), fmt.Sprintf("create database %s", suite.restorationDbName))
	suite.Require().NoError(err, "error creating database")

	restoreDbConn, err := pgx.Connect(context.Background(), fmt.Sprintf("%s dbname=%s", uri, suite.restorationDbName))
	suite.Require().NoError(err, "error connecting to restore db")
	defer restoreDbConn.Close(context.Background())
	_, err = restoreDbConn.Exec(context.Background(), "drop schema public;")
	suite.Require().NoError(err, "error creating database")

	getVersionQuery := `
		select 
		    setting::INT 
		from pg_settings 
		where name = 'server_version_num'
	`

	row := suite.conn.QueryRow(context.Background(), getVersionQuery)
	err = row.Scan(&suite.pgVersionNum)
	suite.Require().NoError(err, "error getting pg version")
	log.Info().Int("version", suite.pgVersionNum).Msg("got pg version")

	suite.configFilePath = path.Join(suite.tmpDir, "config.yaml")
	func() {
		confFile, err := os.Create(suite.configFilePath)
		suite.Require().NoError(err, "error creating config.yaml file")
		defer confFile.Close()
		err = configStr.Execute(
			confFile,
			map[string]any{
				"pgBinPath":  pgBinPath,
				"tmpDir":     suite.tmpDir,
				"uri":        uri,
				"storageDir": suite.storageDir,
				"version":    suite.pgVersionNum,
			})
		suite.Require().NoError(err, "error encoding config into yaml")
	}()
	// Read file and debug
	data, err := os.ReadFile(suite.configFilePath)
	suite.Require().NoError(err, "error reading config file")
	log.Debug().Msg("config file content")
	fmt.Println(string(data))

}

func (suite *BackwardCompatibilitySuite) TestGreenmaskCompatibility() {
	suite.Run("dumping data using greenmask", func() {
		cmd := exec.Command(path.Join(greenmaskBinPath, "greenmask"),
			"--config", suite.configFilePath, "dump",
		)
		log.Debug().Str("cmd", cmd.String()).Msg("running greenmask")
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		log.Info().Str("cmd", cmd.String()).Msg("greenmask stdout and stderr forwarding")

		err := cmd.Run()
		suite.Require().NoError(err, "error running greenmask")
	})

	suite.Run("testing pg_restore list", func() {
		entry, err := os.ReadDir(suite.storageDir)
		suite.Require().NoError(err, "error reading storage directory")
		suite.Require().Len(entry, 1, "unexpected directories in storage")
		lastDump := entry[0]
		suite.Require().True(lastDump.IsDir(), "unable to find last dump dir")

		cmd := exec.Command(path.Join(pgBinPath, "pg_restore"),
			"-l", path.Join(suite.storageDir, lastDump.Name()),
		)
		log.Info().Str("cmd", cmd.String()).Msg("running pg_restore list")
		out, err := cmd.Output()
		if len(out) > 0 {
			log.Info().Msg("pg_restore stout forwarding")
			fmt.Println(string(out))
		}
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				log.Warn().Str("stderr", string(exitErr.Stderr)).Msg("pg_restore run stderr forwarding")
				suite.Assert().NotContains(string(exitErr.Stderr), "warning", "received stderr contains warnings")
				suite.Assert().NotContains(string(exitErr.Stderr), "error", "received stderr contains errors")
			}
			suite.Require().NoError(err, "error performing pg_restore")
		}
	})

	suite.Run("testing pg_restore restoration", func() {

		entry, err := os.ReadDir(suite.storageDir)
		suite.Require().NoError(err, "error reading storage directory")
		suite.Require().Len(entry, 1, "unexpected directories in storage")
		lastDump := entry[0]
		suite.Require().True(lastDump.IsDir(), "unable to find last dump dir")

		cmd := exec.Command(path.Join(pgBinPath, "pg_restore"),
			"-d", fmt.Sprintf("%s dbname=%s", uri, suite.restorationDbName),
			"-v",
			path.Join(suite.storageDir, lastDump.Name()),
		)
		log.Info().Str("cmd", cmd.String()).Msg("running pg_restore")
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		log.Info().Str("cmd", cmd.String()).Msg("pg_restore stdout and stderr forwarding")
		err = cmd.Run()
		suite.Require().NoError(err, "error performing pg_restore")
	})
}

func (suite *BackwardCompatibilitySuite) TearDownSuite() {
	if deleteArtifacts {
		log.Debug().Msg("deleting tmp dir")
		if err := os.RemoveAll(suite.tmpDir); err != nil {
			log.Warn().Err(err).Msg("error deleting tmp dir")
		}
		if suite.conn != nil && suite.restorationDbName != "" {
			_, err := suite.conn.Exec(context.Background(), fmt.Sprintf("drop database %s", suite.restorationDbName))
			if err != nil {
				log.Warn().Err(err).Msg("error droping db")
			}
		}
	} else {
		log.Debug().Str("dir", suite.tmpDir).Msg("keeping artifacts")
	}
	if suite.conn != nil {
		suite.conn.Close(context.Background())
	}

}
