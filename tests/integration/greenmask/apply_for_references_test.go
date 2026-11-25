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

const applyForReferencesConfig = `common:
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
%[4]s`

// applyForReferencesSchema - customers.code is referenced down to the third level.
// customer_label is never transformed and tells which customer a row belonged to.
const applyForReferencesSchema = `
	CREATE TABLE customers
	(
		code  TEXT PRIMARY KEY,
		label TEXT NOT NULL UNIQUE
	);

	-- first level, the FK is not a part of the PK
	CREATE TABLE orders
	(
		id             INTEGER PRIMARY KEY,
		customer_code  TEXT REFERENCES customers (code),
		customer_label TEXT
	);

	-- first level, the FK is the PK
	CREATE TABLE profiles
	(
		customer_code  TEXT PRIMARY KEY REFERENCES customers (code),
		customer_label TEXT NOT NULL
	);

	-- second level, the FK is not a part of the PK
	CREATE TABLE profile_notes
	(
		id             INTEGER PRIMARY KEY,
		profile_code   TEXT REFERENCES profiles (customer_code),
		customer_label TEXT
	);

	-- second level, the FK is the PK
	CREATE TABLE profile_details
	(
		customer_code  TEXT PRIMARY KEY REFERENCES profiles (customer_code),
		customer_label TEXT NOT NULL
	);

	-- third level, the FK is not a part of the PK
	CREATE TABLE detail_notes
	(
		id             INTEGER PRIMARY KEY,
		detail_code    TEXT REFERENCES profile_details (customer_code),
		customer_label TEXT
	);

	INSERT INTO customers (code, label)
	VALUES ('ACC-1', 'first'), ('ACC-2', 'second'), ('ACC-3', 'third');

	INSERT INTO orders (id, customer_code, customer_label)
	VALUES (1, 'ACC-1', 'first'), (2, 'ACC-2', 'second'), (3, 'ACC-3', 'third'), (4, 'ACC-1', 'first'), (5, NULL, NULL);

	INSERT INTO profiles (customer_code, customer_label)
	VALUES ('ACC-1', 'first'), ('ACC-2', 'second'), ('ACC-3', 'third');

	INSERT INTO profile_notes (id, profile_code, customer_label)
	VALUES (1, 'ACC-1', 'first'), (2, 'ACC-2', 'second'), (3, 'ACC-3', 'third'), (4, NULL, NULL);

	INSERT INTO profile_details (customer_code, customer_label)
	VALUES ('ACC-1', 'first'), ('ACC-2', 'second'), ('ACC-3', 'third');

	INSERT INTO detail_notes (id, detail_code, customer_label)
	VALUES (1, 'ACC-1', 'first'), (2, 'ACC-2', 'second'), (3, 'ACC-3', 'third'), (4, NULL, NULL);
`

// applyForReferencesColumns - every column that references customers.code, directly or through a chain.
var applyForReferencesColumns = []struct {
	table  string
	column string
}{
	{table: "orders", column: "customer_code"},
	{table: "profiles", column: "customer_code"},
	{table: "profile_notes", column: "profile_code"},
	{table: "profile_details", column: "customer_code"},
	{table: "detail_notes", column: "detail_code"},
}

var applyForReferencesCases = []struct {
	name           string
	transformation string
	// masked - source codes that must not be left in any column
	masked []string
}{
	{
		// issue #571
		name: "Hash",
		transformation: `    - schema: "public"
      name: "customers"
      transformers:
        - name: "Hash"
          apply_for_references: true
          params:
            column: "code"
            salt: "0123456789abcdef0123456789abcdef"
`,
		masked: []string{"ACC-1", "ACC-2", "ACC-3"},
	},
	{
		// issue #571
		name: "RegexpReplace",
		transformation: `    - schema: "public"
      name: "customers"
      transformers:
        - name: "RegexpReplace"
          apply_for_references: true
          params:
            column: "code"
            regexp: "^ACC-"
            replace: "CUS-"
`,
		masked: []string{"ACC-1", "ACC-2", "ACC-3"},
	},
	{
		// issue #571, the condition keeps the primary key unique and has to be inherited at every level
		name: "Replace with when",
		transformation: `    - schema: "public"
      name: "customers"
      transformers:
        - name: "Replace"
          apply_for_references: true
          when: 'record.code == "ACC-1"'
          params:
            column: "code"
            value: "REDACTED"
`,
		masked: []string{"ACC-1"},
	},
	{
		name: "RandomString with hash engine",
		transformation: `    - schema: "public"
      name: "customers"
      transformers:
        - name: "RandomString"
          apply_for_references: true
          params:
            column: "code"
            engine: "hash"
            min_length: 10
            max_length: 10
`,
		masked: []string{"ACC-1", "ACC-2", "ACC-3"},
	},
	{
		// profiles takes the chain over, the tables below it must not be transformed twice
		name: "Hash set manually in the middle of the chain",
		transformation: `    - schema: "public"
      name: "customers"
      transformers:
        - name: "Hash"
          apply_for_references: true
          params:
            column: "code"
            salt: "0123456789abcdef0123456789abcdef"
    - schema: "public"
      name: "profiles"
      transformers:
        - name: "Hash"
          apply_for_references: true
          params:
            column: "customer_code"
            salt: "0123456789abcdef0123456789abcdef"
`,
		masked: []string{"ACC-1", "ACC-2", "ACC-3"},
	},
}

// ApplyForReferencesSuite - regression suite for apply_for_references: a transformer on a primary key
// must reach every referencing column, at any depth, exactly once.
type ApplyForReferencesSuite struct {
	suite.Suite
	tmpDir       string
	conn         *pgx.Conn
	dbConfig     *pgx.ConnConfig
	sourceDbName string
	restoreDbs   []string
}

func (suite *ApplyForReferencesSuite) SetupSuite() {
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	ctx := context.Background()

	var err error
	suite.dbConfig, err = pgx.ParseConfig(uri)
	suite.Require().NoError(err)

	suite.tmpDir, err = os.MkdirTemp(tempDir, "apply_for_references_test_")
	suite.Require().NoError(err)

	suite.conn, err = pgx.ConnectConfig(ctx, suite.dbConfig)
	suite.Require().NoError(err)

	suite.sourceDbName = fmt.Sprintf("apply_for_references_source_%d", time.Now().UnixMilli())
	_, err = suite.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", suite.sourceDbName))
	suite.Require().NoError(err)

	sourceConn := suite.connectTo(ctx, suite.sourceDbName)
	defer sourceConn.Close(ctx) // nolint: errcheck
	_, err = sourceConn.Exec(ctx, applyForReferencesSchema)
	suite.Require().NoError(err)
}

func (suite *ApplyForReferencesSuite) connectTo(ctx context.Context, dbName string) *pgx.Conn {
	cfg := suite.dbConfig.Copy()
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	suite.Require().NoError(err)
	return conn
}

func (suite *ApplyForReferencesSuite) runGreenmask(configPath, dbName string, args ...string) error {
	greenmaskBin := path.Join(greenmaskBinPath, "greenmask")
	cmd := exec.Command(greenmaskBin, append([]string{"--config", configPath}, args...)...)

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
	return err
}

// dumpAndRestore - dumps the source database with the given transformation and restores it into a new one.
func (suite *ApplyForReferencesSuite) dumpAndRestore(ctx context.Context, idx int, transformation string) string {
	caseDir := path.Join(suite.tmpDir, fmt.Sprintf("case_%d", idx))
	storageDir := path.Join(caseDir, "storage")
	suite.Require().NoError(os.MkdirAll(storageDir, 0700))

	configPath := path.Join(caseDir, "config.yml")
	cfg := fmt.Sprintf(applyForReferencesConfig, pgBinPath, caseDir, storageDir, transformation)
	suite.Require().NoError(os.WriteFile(configPath, []byte(cfg), 0600))

	restoreDbName := fmt.Sprintf("%s_restore_%d", suite.sourceDbName, idx)
	_, err := suite.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", restoreDbName))
	suite.Require().NoError(err)
	suite.restoreDbs = append(suite.restoreDbs, restoreDbName)

	suite.Require().NoError(suite.runGreenmask(configPath, suite.sourceDbName, "dump"), "dump failed")
	suite.Require().NoError(suite.runGreenmask(configPath, restoreDbName, "restore", "latest"), "restore failed")
	return restoreDbName
}

func (suite *ApplyForReferencesSuite) count(ctx context.Context, conn *pgx.Conn, query string, args ...any) int {
	var res int
	suite.Require().NoError(conn.QueryRow(ctx, query, args...).Scan(&res), query)
	return res
}

func (suite *ApplyForReferencesSuite) TestReferencesAreTransformed() {
	ctx := context.Background()

	sourceConn := suite.connectTo(ctx, suite.sourceDbName)
	defer sourceConn.Close(ctx) // nolint: errcheck

	for idx, tc := range applyForReferencesCases {
		suite.Run(tc.name, func() {
			restoreDbName := suite.dumpAndRestore(ctx, idx, tc.transformation)
			conn := suite.connectTo(ctx, restoreDbName)
			defer conn.Close(ctx) // nolint: errcheck

			suite.Assert().Equal(3, suite.count(ctx, conn, "SELECT count(*) FROM customers"))
			suite.Assert().Zero(
				suite.count(ctx, conn, "SELECT count(*) FROM customers WHERE code = ANY ($1)", tc.masked),
				"customers.code is not transformed",
			)

			for _, ref := range applyForReferencesColumns {
				name := fmt.Sprintf("%s.%s", ref.table, ref.column)

				countQuery := fmt.Sprintf("SELECT count(*) FROM %s", ref.table)
				suite.Assert().Equal(
					suite.count(ctx, sourceConn, countQuery), suite.count(ctx, conn, countQuery),
					"%s: rows are lost", name,
				)

				suite.Assert().Zero(
					suite.count(ctx, conn,
						fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = ANY ($1)", ref.table, ref.column), tc.masked,
					),
					"%s keeps the source value", name,
				)

				// A row must point at the customer it belonged to; NULL stays NULL.
				suite.Assert().Zero(
					suite.count(ctx, conn, fmt.Sprintf(`
						SELECT count(*)
						FROM %[1]s r
								 LEFT JOIN customers c ON c.label = r.customer_label
						WHERE r.%[2]s IS DISTINCT FROM c.code
					`, ref.table, ref.column)),
					"%s differs from customers.code", name,
				)
			}

			suite.Assert().Equal(
				len(applyForReferencesColumns),
				suite.count(ctx, conn, "SELECT count(*) FROM pg_catalog.pg_constraint WHERE contype = 'f'"),
				"foreign keys are not restored",
			)
		})
	}
}

func (suite *ApplyForReferencesSuite) TearDownSuite() {
	ctx := context.Background()
	if suite.conn != nil {
		for _, dbName := range append(suite.restoreDbs, suite.sourceDbName) {
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
