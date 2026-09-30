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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
)

// copyEscapeValues - values that must survive a transformer reading them unchanged.
// \. and \N followed by a byte were truncated by the COPY encoder (issue #489).
// A bare \N is excluded since Template treats this output as NULL.
var copyEscapeValues = []string{
	`a\.b`,
	`a\.[0-9]b`,
	`a\.é`,
	`a\Nb`,
	`a\.`,
	`\.`,
	`\\.`,
	`\\N`,
	`a\,b`,
	`C:\Users\x`,
	`a\zb`,
	`a\1b`,
	`a\ b`,
	`a\"b`,
	"a\x01b",
	"tab\there\nnew line\r\\",
}

// copyEscapeDirective - the Apache directive from the issue report that broke the jsonb restore.
const copyEscapeDirective = `<FilesMatch \"\\.[0-9a-f]{12}\\.(css|js)$\">`

const copyEscapeConfig = `common:
  pg_bin_path: %[1]q
  tmp_dir: %[2]q
log:
  level: "info"
  format: "text"
storage:
  directory:
    path: %[3]q
dump:
  transformation:
    - schema: "public"
      name: "copy_escape"
      transformers:
        - name: "Template"
          params:
            column: "t"
            template: "{{ .GetRawValue }}"
        - name: "Template"
          params:
            column: "vc"
            template: "{{ .GetRawValue }}"
        - name: "Json"
          params:
            column: "j"
            operations:
              - operation: "set"
                path: "masked"
                value: true
        - name: "Template"
          params:
            column: "tpl_j"
            template: '{{ .GetRawValue | jsonSet "masked" true }}'
`

// CopyEscapeSuite - regression suite for values containing COPY escape sequences
// that are read and written back by transformers.
type CopyEscapeSuite struct {
	suite.Suite
	tmpDir        string
	storageDir    string
	configPath    string
	conn          *pgx.Conn
	dbConfig      *pgx.ConnConfig
	sourceDbName  string
	restoreDbName string
}

func (suite *CopyEscapeSuite) SetupSuite() {
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	ctx := context.Background()

	var err error
	suite.dbConfig, err = pgx.ParseConfig(uri)
	suite.Require().NoError(err)

	suite.tmpDir, err = os.MkdirTemp(tempDir, "copy_escape_test_")
	suite.Require().NoError(err)

	suite.storageDir = path.Join(suite.tmpDir, "storage")
	suite.Require().NoError(os.Mkdir(suite.storageDir, 0700))

	suite.configPath = path.Join(suite.tmpDir, "config.yml")
	cfg := fmt.Sprintf(copyEscapeConfig, pgBinPath, suite.tmpDir, suite.storageDir)
	suite.Require().NoError(os.WriteFile(suite.configPath, []byte(cfg), 0600))

	suite.conn, err = pgx.ConnectConfig(ctx, suite.dbConfig)
	suite.Require().NoError(err)

	suffix := time.Now().UnixMilli()
	suite.sourceDbName = fmt.Sprintf("copy_escape_source_%d", suffix)
	suite.restoreDbName = fmt.Sprintf("copy_escape_restore_%d", suffix)

	for _, dbName := range []string{suite.sourceDbName, suite.restoreDbName} {
		_, err = suite.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s ENCODING 'UTF8' TEMPLATE template0", dbName))
		suite.Require().NoError(err)
	}

	sourceConn := suite.connectTo(ctx, suite.sourceDbName)
	defer sourceConn.Close(ctx) // nolint: errcheck

	_, err = sourceConn.Exec(ctx, `
		CREATE TABLE copy_escape
		(
			id    INTEGER PRIMARY KEY,
			t     TEXT         NOT NULL,
			vc    VARCHAR(256) NOT NULL,
			j     JSONB        NOT NULL,
			tpl_j JSONB        NOT NULL,
			raw   TEXT         NOT NULL
		);
	`)
	suite.Require().NoError(err)

	// Values are passed as parameters so that no SQL-level escaping is involved
	for i, v := range copyEscapeValues {
		doc, err := json.Marshal(map[string]string{"v": v, "directive": copyEscapeDirective})
		suite.Require().NoError(err)
		_, err = sourceConn.Exec(ctx,
			"INSERT INTO copy_escape (id, t, vc, j, tpl_j, raw) VALUES ($1, $2::TEXT, $2::TEXT, $3::JSONB, $3::JSONB, $2::TEXT)",
			i+1, v, string(doc),
		)
		suite.Require().NoError(err)
	}
}

func (suite *CopyEscapeSuite) connectTo(ctx context.Context, dbName string) *pgx.Conn {
	cfg := suite.dbConfig.Copy()
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	suite.Require().NoError(err)
	return conn
}

func (suite *CopyEscapeSuite) runGreenmask(dbName string, args ...string) (string, error) {
	greenmaskBin := path.Join(greenmaskBinPath, "greenmask")
	cmd := exec.Command(greenmaskBin, append([]string{"--config", suite.configPath}, args...)...)

	env := append(os.Environ(),
		fmt.Sprintf("PGDATABASE=%s", dbName),
		fmt.Sprintf("PGHOST=%s", suite.dbConfig.Host),
		fmt.Sprintf("PGPORT=%d", suite.dbConfig.Port),
		fmt.Sprintf("PGUSER=%s", suite.dbConfig.User),
		fmt.Sprintf("PGPASSWORD=%s", suite.dbConfig.Password),
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

type copyEscapeRow struct {
	t, vc, j, tplJ, raw string
	jMasked, tplJMasked string
}

// readRows - reads rows keyed by id; jsonb columns are returned without the "masked" key
// so that they can be compared with the source.
func (suite *CopyEscapeSuite) readRows(ctx context.Context, dbName string) map[int]copyEscapeRow {
	conn := suite.connectTo(ctx, dbName)
	defer conn.Close(ctx) // nolint: errcheck

	rows, err := conn.Query(ctx, `
		SELECT id, t, vc, (j - 'masked')::TEXT, (tpl_j - 'masked')::TEXT, raw,
		       COALESCE((j -> 'masked')::TEXT, ''), COALESCE((tpl_j -> 'masked')::TEXT, '')
		FROM copy_escape
	`)
	suite.Require().NoError(err)
	defer rows.Close()

	res := make(map[int]copyEscapeRow)
	for rows.Next() {
		var id int
		var r copyEscapeRow
		suite.Require().NoError(rows.Scan(&id, &r.t, &r.vc, &r.j, &r.tplJ, &r.raw, &r.jMasked, &r.tplJMasked))
		res[id] = r
	}
	suite.Require().NoError(rows.Err())
	return res
}

func (suite *CopyEscapeSuite) TestTransformedValuesArePreserved() {
	ctx := context.Background()

	_, err := suite.runGreenmask(suite.sourceDbName, "dump")
	suite.Require().NoError(err, "dump failed")

	_, err = suite.runGreenmask(suite.restoreDbName, "restore", "latest")
	suite.Require().NoError(err, "restore failed")

	source := suite.readRows(ctx, suite.sourceDbName)
	restored := suite.readRows(ctx, suite.restoreDbName)
	suite.Require().Len(restored, len(copyEscapeValues))

	for id, src := range source {
		dst, ok := restored[id]
		suite.Require().True(ok, "row %d is missing", id)
		v := copyEscapeValues[id-1]
		suite.Assert().Equal(src.t, dst.t, "text column, value %q", v)
		suite.Assert().Equal(src.vc, dst.vc, "varchar column, value %q", v)
		suite.Assert().Equal(src.j, dst.j, "jsonb column with Json set, value %q", v)
		suite.Assert().Equal(src.tplJ, dst.tplJ, "jsonb column with jsonSet, value %q", v)
		suite.Assert().Equal(src.raw, dst.raw, "untransformed column, value %q", v)
		suite.Assert().Equal("true", dst.jMasked, "Json set was not applied, value %q", v)
		suite.Assert().Equal("true", dst.tplJMasked, "jsonSet was not applied, value %q", v)
	}
}

func (suite *CopyEscapeSuite) TearDownSuite() {
	ctx := context.Background()
	if suite.conn != nil {
		for _, dbName := range []string{suite.sourceDbName, suite.restoreDbName} {
			if dbName == "" {
				continue
			}
			suite.conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName)) // nolint: errcheck
		}
		suite.conn.Close(ctx) // nolint: errcheck
	}
	if suite.tmpDir != "" {
		os.RemoveAll(suite.tmpDir) // nolint: errcheck
	}
}
