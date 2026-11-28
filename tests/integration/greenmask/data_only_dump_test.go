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
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"

	toclib "github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

const dataOnlyConfig = `common:
  pg_bin_path: %[1]q
  tmp_dir: %[2]q
log:
  level: "info"
  format: "text"
storage:
  directory:
    path: %[3]q
dump:
  pg_dump_options:
%[4]s
  transformation:
    - schema: "public"
      name: "customers"
      transformers:
        - name: "Replace"
          params:
            column: "email"
            value: "masked@example.com"
`

const dataOnlyMaskedEmail = "masked@example.com"

// dataOnlyFixture covers what a data-only archive carries: plain, partitioned and generated-column tables,
// a foreign key, sequences and a large object.
const dataOnlyFixture = `
	CREATE EXTENSION IF NOT EXISTS pgcrypto;
	CREATE SCHEMA app;

	CREATE TABLE public.customers
	(
		id    SERIAL PRIMARY KEY,
		email TEXT NOT NULL
	);

	CREATE TABLE app.orders
	(
		id          INTEGER PRIMARY KEY,
		customer_id INTEGER NOT NULL REFERENCES public.customers (id),
		amount      NUMERIC NOT NULL,
		doubled     NUMERIC GENERATED ALWAYS AS (amount * 2) STORED
	);

	CREATE TABLE app.events
	(
		id      INTEGER,
		created DATE NOT NULL
	) PARTITION BY RANGE (created);
	CREATE TABLE app.events_2024 PARTITION OF app.events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
	CREATE TABLE app.events_2025 PARTITION OF app.events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

	CREATE SEQUENCE app.counter;
	SELECT setval('app.counter', 42);

	CREATE PUBLICATION data_only_pub FOR TABLE public.customers;

	INSERT INTO public.customers (email)
	SELECT 'user' || g || '@example.org' FROM generate_series(1, 10) g;
	INSERT INTO app.orders (id, customer_id, amount)
	SELECT g, (g % 10) + 1, g * 1.5 FROM generate_series(1, 20) g;
	INSERT INTO app.events (id, created)
	VALUES (1, '2024-03-01'), (2, '2024-07-01'), (3, '2025-02-01');

	SELECT lo_from_bytea(0, 'hello large object');
`

type DataOnlyDumpSuite struct {
	suite.Suite
	tmpDir       string
	conn         *pgx.Conn
	dbConfig     *pgx.ConnConfig
	sourceDbName string
	createdDbs   []string
	caseIdx      int
}

func (suite *DataOnlyDumpSuite) SetupSuite() {
	suite.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	suite.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	suite.Require().NotEmpty(uri, "-uri non-empty flag required")
	suite.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")

	ctx := context.Background()

	var err error
	suite.dbConfig, err = pgx.ParseConfig(uri)
	suite.Require().NoError(err)

	suite.tmpDir, err = os.MkdirTemp(tempDir, "data_only_dump_test_")
	suite.Require().NoError(err)

	suite.conn, err = pgx.ConnectConfig(ctx, suite.dbConfig)
	suite.Require().NoError(err)

	suite.sourceDbName = fmt.Sprintf("data_only_source_%d", time.Now().UnixMilli())
	suite.createDb(ctx, suite.sourceDbName)

	sourceConn := suite.connectTo(ctx, suite.sourceDbName)
	defer sourceConn.Close(ctx) // nolint: errcheck
	_, err = sourceConn.Exec(ctx, dataOnlyFixture)
	suite.Require().NoError(err)
	// Database-level objects: pg_dump --data-only keeps the settings and drops the comment.
	_, err = sourceConn.Exec(ctx, fmt.Sprintf(
		"ALTER DATABASE %[1]s SET work_mem = '8MB'; COMMENT ON DATABASE %[1]s IS 'data-only source'", suite.sourceDbName,
	))
	suite.Require().NoError(err)
}

func (suite *DataOnlyDumpSuite) TearDownSuite() {
	ctx := context.Background()
	if suite.conn != nil {
		for _, dbName := range suite.createdDbs {
			suite.conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName)) // nolint: errcheck
		}
		suite.conn.Close(ctx) // nolint: errcheck
	}
	if suite.tmpDir != "" && deleteArtifacts {
		os.RemoveAll(suite.tmpDir) // nolint: errcheck
	}
}

// TestDump - data-only works from the config and from the CLI flag.
func (suite *DataOnlyDumpSuite) TestDump() {
	cases := []struct {
		name    string
		options string
		args    []string
	}{
		{name: "config option", options: "    data-only: true"},
		{name: "cli flag", args: []string{"--data-only"}},
	}
	for _, tc := range cases {
		suite.Run(tc.name, func() {
			c := suite.newCase(tc.options)
			out, err := suite.runGreenmask(c.configPath, suite.sourceDbName, append([]string{"dump"}, tc.args...)...)
			suite.Require().NoError(err, out)

			tocObj := readToc(suite.T(), c.latestDumpDir())
			for _, e := range tocObj.Entries {
				suite.Assert().NotEqualf(toclib.SectionPostData, e.Section, "unexpected post-data entry %s", entryKey(e))
				if e.Section == toclib.SectionPreData {
					suite.Assert().Truef(
						isHeaderEntry(e) || isLargeObjectEntry(e),
						"unexpected pre-data entry %s", entryKey(e),
					)
				}
			}
		})
	}
}

// TestTocMatchesNative - the archive must look like the one native pg_dump --data-only writes.
func (suite *DataOnlyDumpSuite) TestTocMatchesNative() {
	cases := []struct {
		name       string
		options    string
		nativeArgs []string
	}{
		{name: "default", options: "    data-only: true"},
		{name: "encoding", options: "    data-only: true\n    encoding: LATIN1", nativeArgs: []string{"--encoding", "LATIN1"}},
		{name: "create", options: "    data-only: true\n    create: true", nativeArgs: []string{"--create"}},
	}
	for _, tc := range cases {
		suite.Run(tc.name, func() {
			c := suite.newCase(tc.options)
			out, err := suite.runGreenmask(c.configPath, suite.sourceDbName, "dump")
			suite.Require().NoError(err, out)
			got := readToc(suite.T(), c.latestDumpDir())

			nativeDir := path.Join(c.dir, "native")
			suite.runPg("pg_dump", append([]string{
				"-Fd", "--data-only", "-f", nativeDir, "--dbname", suite.dsn(suite.sourceDbName),
			}, tc.nativeArgs...)...)
			want := readToc(suite.T(), nativeDir)

			suite.Assert().Equal(headerKey(want.Header), headerKey(got.Header), "toc header")
			suite.Assert().Equal(headerEntries(want), headerEntries(got), "entries without data")
			suite.Assert().Equal(dataEntries(want), dataEntries(got), "data entries")

			present := make(map[int32]bool, len(got.Entries))
			for _, e := range got.Entries {
				present[e.DumpId] = true
			}
			for _, e := range got.Entries {
				for _, dep := range e.Dependencies {
					suite.Assert().Truef(present[dep], "%s depends on missing entry %d", entryKey(e), dep)
				}
			}
		})
	}
}

// TestNativePgRestore - native pg_restore loads the archive between the pre-data and post-data sections
// of the schema, the usual way to restore a data-only dump.
func (suite *DataOnlyDumpSuite) TestNativePgRestore() {
	ctx := context.Background()
	c := suite.newCase("    data-only: true")
	out, err := suite.runGreenmask(c.configPath, suite.sourceDbName, "dump")
	suite.Require().NoError(err, out)

	target := suite.newTarget(ctx, c, "pre-data")
	suite.runPg("pg_restore", "--exit-on-error", "--dbname", suite.dsn(target), c.latestDumpDir())
	suite.applySchema(c, target, "post-data")
	suite.assertRestored(ctx, target, true)
}

// TestGreenmaskRestore - greenmask restore loads the archive into a database that has the schema.
// restore --data-only skips the pre-data section, where large objects are created, for any dump,
// so that case runs on a dump without large objects.
func (suite *DataOnlyDumpSuite) TestGreenmaskRestore() {
	ctx := context.Background()
	cases := []struct {
		name    string
		options string
		args    []string
		blobs   bool
	}{
		{name: "without flags", options: "    data-only: true", blobs: true},
		{name: "data-only flag", options: "    data-only: true\n    no-blobs: true", args: []string{"--data-only"}},
	}
	for _, tc := range cases {
		suite.Run(tc.name, func() {
			c := suite.newCase(tc.options)
			out, err := suite.runGreenmask(c.configPath, suite.sourceDbName, "dump")
			suite.Require().NoError(err, out)

			target := suite.newTarget(ctx, c, "pre-data", "post-data")
			args := append([]string{"restore", "latest"}, tc.args...)
			out, err = suite.runGreenmask(c.configPath, target, args...)
			suite.Require().NoError(err, out)
			suite.assertRestored(ctx, target, tc.blobs)
		})
	}
}

// TestConflictingOptionsRejected - greenmask reports the conflict itself, before pg_dump runs.
func (suite *DataOnlyDumpSuite) TestConflictingOptionsRejected() {
	cases := []struct {
		name    string
		options string
		args    []string
		want    string
	}{
		{
			name: "schema-only", options: "    data-only: true\n    schema-only: true",
			want: "options --data-only and --schema-only cannot be used together",
		},
		{
			name: "clean flag", args: []string{"--data-only", "--clean"},
			want: "options --data-only and --clean cannot be used together",
		},
	}
	for _, tc := range cases {
		suite.Run(tc.name, func() {
			c := suite.newCase(tc.options)
			out, err := suite.runGreenmask(c.configPath, suite.sourceDbName, append([]string{"dump"}, tc.args...)...)
			suite.Require().Error(err, out)
			suite.Assert().Contains(out, tc.want)
			suite.Assert().NotContains(out, "pg_dump: error")

			dumps, err := os.ReadDir(c.storageDir)
			suite.Require().NoError(err)
			suite.Assert().Empty(dumps, "no dump must be left in the storage")
		})
	}
}

type dataOnlyCase struct {
	dir        string
	storageDir string
	configPath string
}

func (c *dataOnlyCase) latestDumpDir() string {
	dumps, err := os.ReadDir(c.storageDir)
	if err != nil || len(dumps) == 0 {
		return path.Join(c.storageDir, "missing")
	}
	return path.Join(c.storageDir, dumps[len(dumps)-1].Name())
}

func (suite *DataOnlyDumpSuite) newCase(options string) *dataOnlyCase {
	suite.caseIdx++
	c := &dataOnlyCase{dir: path.Join(suite.tmpDir, fmt.Sprintf("case_%d", suite.caseIdx))}
	c.storageDir = path.Join(c.dir, "storage")
	suite.Require().NoError(os.MkdirAll(c.storageDir, 0700))

	if options == "" {
		options = "    {}"
	}
	c.configPath = path.Join(c.dir, "config.yml")
	cfg := fmt.Sprintf(dataOnlyConfig, pgBinPath, c.dir, c.storageDir, options)
	suite.Require().NoError(os.WriteFile(c.configPath, []byte(cfg), 0600))
	return c
}

func (suite *DataOnlyDumpSuite) createDb(ctx context.Context, name string) {
	_, err := suite.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", name))
	suite.Require().NoError(err)
	suite.createdDbs = append(suite.createdDbs, name)
}

// newTarget - creates a database and applies the given schema sections of the source to it.
func (suite *DataOnlyDumpSuite) newTarget(ctx context.Context, c *dataOnlyCase, sections ...string) string {
	suite.caseIdx++
	target := fmt.Sprintf("%s_target_%d", suite.sourceDbName, suite.caseIdx)
	suite.createDb(ctx, target)
	for _, section := range sections {
		suite.applySchema(c, target, section)
	}
	return target
}

// applySchema - applies one schema section of the source, without large objects, to the target.
func (suite *DataOnlyDumpSuite) applySchema(c *dataOnlyCase, target, section string) {
	suite.caseIdx++
	schemaFile := path.Join(c.dir, fmt.Sprintf("schema_%d.sql", suite.caseIdx))
	suite.runPg("pg_dump", "--section", section, "--no-blobs", "-f", schemaFile,
		"--dbname", suite.dsn(suite.sourceDbName),
	)
	suite.runPg("psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", schemaFile, "--dbname", suite.dsn(target))
}

func (suite *DataOnlyDumpSuite) assertRestored(ctx context.Context, target string, blobs bool) {
	source := suite.connectTo(ctx, suite.sourceDbName)
	defer source.Close(ctx) // nolint: errcheck
	conn := suite.connectTo(ctx, target)
	defer conn.Close(ctx) // nolint: errcheck

	for _, query := range []string{
		"SELECT count(*) FROM public.customers",
		"SELECT count(*) FROM app.orders",
		"SELECT count(*) FROM app.orders WHERE doubled = amount * 2",
		"SELECT count(*) FROM app.events",
		"SELECT count(*) FROM app.events_2024",
		"SELECT last_value FROM app.counter",
		"SELECT last_value FROM public.customers_id_seq",
	} {
		suite.Assert().Equal(suite.count(ctx, source, query), suite.count(ctx, conn, query), query)
	}

	suite.Assert().EqualValues(10,
		suite.count(ctx, conn, "SELECT count(*) FROM public.customers WHERE email = $1", dataOnlyMaskedEmail),
		"customers.email is not transformed",
	)

	if !blobs {
		suite.Assert().Zero(suite.count(ctx, conn, "SELECT count(*) FROM pg_catalog.pg_largeobject_metadata"))
		return
	}
	suite.Assert().EqualValues(1, suite.count(ctx, conn, "SELECT count(*) FROM pg_catalog.pg_largeobject_metadata"))
	var content string
	err := conn.QueryRow(ctx,
		"SELECT convert_from(lo_get(oid), 'UTF8') FROM pg_catalog.pg_largeobject_metadata",
	).Scan(&content)
	suite.Require().NoError(err)
	suite.Assert().Equal("hello large object", content)
}

func (suite *DataOnlyDumpSuite) connectTo(ctx context.Context, dbName string) *pgx.Conn {
	cfg := suite.dbConfig.Copy()
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	suite.Require().NoError(err)
	return conn
}

func (suite *DataOnlyDumpSuite) count(ctx context.Context, conn *pgx.Conn, query string, args ...any) int64 {
	var res int64
	suite.Require().NoError(conn.QueryRow(ctx, query, args...).Scan(&res), query)
	return res
}

func (suite *DataOnlyDumpSuite) dsn(dbName string) string {
	return fmt.Sprintf("%s dbname=%s", uri, dbName)
}

func (suite *DataOnlyDumpSuite) runPg(name string, args ...string) {
	cmd := exec.Command(path.Join(pgBinPath, name), args...)
	out, err := cmd.CombinedOutput()
	suite.Require().NoError(err, "%s %v:\n%s", name, args, out)
}

func (suite *DataOnlyDumpSuite) runGreenmask(configPath, dbName string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, path.Join(greenmaskBinPath, "greenmask"), append([]string{"--config", configPath}, args...)...)
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

func readToc(t interface {
	Errorf(format string, args ...any)
	FailNow()
}, dir string) *toclib.Toc {
	f, err := os.Open(path.Join(dir, "toc.dat"))
	if err != nil {
		t.Errorf("open toc.dat: %v", err)
		t.FailNow()
	}
	defer f.Close() // nolint: errcheck
	res, err := toclib.NewReader(f).Read()
	if err != nil {
		t.Errorf("read toc.dat: %v", err)
		t.FailNow()
	}
	return res
}

// headerKey - the header fields that pg_dump decides. The creation time differs between runs.
func headerKey(h *toclib.Header) string {
	str := func(s *string) string {
		if s == nil {
			return "<nil>"
		}
		return *s
	}
	return fmt.Sprintf("version=%d.%d.%d int=%d off=%d format=%d compression=%+v db=%s server=%s dumper=%s",
		h.VersionMajor, h.VersionMinor, h.VersionRev, h.IntSize, h.OffSize, h.Format, h.CompressionSpec,
		str(h.ArchDbName), str(h.ArchiveRemoteVersion), str(h.ArchiveDumpVersion),
	)
}

// headerEntries - every entry that holds no data, in the archive order. They come from pg_dump as is,
// so they must be the same as in the native archive.
func headerEntries(t *toclib.Toc) []string {
	var res []string
	for _, e := range t.Entries {
		if !isDataEntry(e) && !isLargeObjectEntry(e) {
			defn := ""
			if e.Defn != nil {
				defn = *e.Defn
			}
			res = append(res, fmt.Sprintf("%s %q", entryKey(e), defn))
		}
	}
	return res
}

// dataEntries - sorted keys of the data entries. Greenmask quotes the tag and the namespace,
// pg_dump does not, so the quotes are dropped. Large objects are left out: greenmask writes them in
// its own layout (BLOB in pre-data, one BLOBS entry), which differs from pg_dump in every mode.
func dataEntries(t *toclib.Toc) []string {
	var res []string
	for _, e := range t.Entries {
		if isDataEntry(e) {
			res = append(res, entryKey(e))
		}
	}
	slices.Sort(res)
	return res
}

func entryKey(e *toclib.Entry) string {
	str := func(s *string) string {
		if s == nil {
			return ""
		}
		return strings.Trim(*s, `"`)
	}
	return fmt.Sprintf("%s|%s|%s|%s", toclib.SectionMap[e.Section], str(e.Desc), str(e.Namespace), str(e.Tag))
}

func isDataEntry(e *toclib.Entry) bool {
	return e.Desc != nil && (*e.Desc == toclib.TableDataDesc || *e.Desc == toclib.SequenceSetDesc)
}

// isHeaderEntry - the entries without data that pg_dump --data-only writes.
func isHeaderEntry(e *toclib.Entry) bool {
	if e.Desc == nil {
		return false
	}
	switch *e.Desc {
	case "ENCODING", "STDSTRINGS", "SEARCHPATH", "DATABASE", "DATABASE PROPERTIES":
		return true
	}
	return false
}

func isLargeObjectEntry(e *toclib.Entry) bool {
	if e.Desc == nil {
		return false
	}
	switch *e.Desc {
	case toclib.LargeObjectsDesc, toclib.BlobsDesc, "BLOB METADATA":
		return true
	case toclib.CommentDesc, toclib.AclDesc:
		return e.Tag != nil && strings.HasPrefix(*e.Tag, "LARGE OBJECT ")
	}
	return false
}
