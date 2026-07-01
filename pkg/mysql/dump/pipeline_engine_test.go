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

package dump

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// pipelineEngineSuite drives the v2 dump pipeline (NewDumpPipeline) end-to-end
// against a live MySQL server: discovery, planning, schema (DDL) dump, data dump
// (raw and transformed), and metadata persistence.
type pipelineEngineSuite struct {
	testutils.MySQLContainerSuite
}

func (s *pipelineEngineSuite) SetupSuite() {
	// Pin to 8.0 to match the host mysqldump client used by the schema dumper.
	s.SetImage("mysql:8.0")
	// Match the flavor-agnostic "port: 3306" startup line (see introspect_engine_test.go).
	s.SetContainerOptions(testcontainers.CustomizeRequestOption(
		func(req *testcontainers.GenericContainerRequest) error {
			req.WaitingFor = wait.ForLog("port: 3306").WithStartupTimeout(3 * time.Minute)
			return nil
		},
	))
	s.MySQLContainerSuite.SetupSuite()
}

func (s *pipelineEngineSuite) TearDownSuite() {
	s.MySQLContainerSuite.TearDownSuite()
}

func TestPipelineEngineMySQL(t *testing.T) {
	suite.Run(t, new(pipelineEngineSuite))
}

// baseConfig builds a dump config pointing at the container, scoped to the
// testdb schema, with directory storage and no transformers.
func (s *pipelineEngineSuite) baseConfig(ctx context.Context, storageDir string) config.Config {
	opts := s.GetRootConnectionOpts(ctx)
	opts.ConnectDatabase = "testdb"

	cfg := config.Config{Engine: core.DBMSEngineMySQL}
	cfg.Storage = config.NewStorageConfig()
	cfg.Storage.Type = "directory"
	cfg.Storage.Directory.Path = storageDir

	cfg.Dump.Options.Jobs = 1
	cfg.Dump.Options.IncludeSchema = []string{"testdb"}
	cfg.Dump.MysqlConfig.ConnectionOpts = opts
	cfg.Dump.MysqlConfig.DumpFormat = core.DumpFormatInsert
	return cfg
}

func (s *pipelineEngineSuite) seedUsers(ctx context.Context) func() {
	s.MigrateUp(ctx, []string{
		`CREATE TABLE testdb.users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL);`,
		`INSERT INTO testdb.users (name) VALUES ('alice'), ('bob'), ('carol');`,
	})
	return func() { s.MigrateDown(ctx, []string{`DROP TABLE testdb.users;`}) }
}

// TestDiscovery verifies the runtime + discovery half of the pipeline against a
// live server: connection configuration, session open, introspection,
// dependency-graph building, previous-metadata load and subset building.
func (s *pipelineEngineSuite) TestDiscovery() {
	ctx := context.Background()
	down := s.seedUsers(ctx)
	defer down()

	cfg := s.baseConfig(ctx, s.T().TempDir())
	p := NewDumpPipeline()

	runtime, err := p.OpenRuntime(ctx, cfg)
	s.Require().NoError(err, "open runtime (connection + session) should succeed")
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runtime.Close(closeCtx)
	}()

	state := p.NewRun(cfg)
	s.Require().NoError(p.Discover(ctx, runtime, state), "discovery should succeed against live MySQL")

	s.Require().NotNil(state.Discovery.Introspection)
	s.Require().NotNil(state.Discovery.DependencyGraph)
	s.Require().NotNil(state.Discovery.Subset)

	// The seeded users table is introspected under the engine-specific table kind.
	tables := state.Discovery.Introspection.KindsMap[kinds.ObjectKindTable]
	var found bool
	for _, t := range tables {
		if t.Name == "users" {
			found = true
		}
	}
	s.True(found, "introspection should discover the users table under ObjectKindMysqlTable")
}

// TestFullRawDump runs the complete pipeline (RunDump) for an untransformed
// table and verifies the produced storage artifacts: schema DDL (pre/post-data),
// table data, and metadata.json.
func (s *pipelineEngineSuite) TestFullRawDump() {
	ctx := context.Background()
	down := s.seedUsers(ctx)
	defer down()

	storageDir := s.T().TempDir()
	cfg := s.baseConfig(ctx, storageDir)

	_, err := NewDumpPipeline().RunDump(ctx, cfg)
	s.Require().NoError(err, "full RunDump should succeed end-to-end")

	files := s.storageFiles(storageDir)
	s.Contains(files, "schema_pre_testdb.sql", "pre-data schema DDL should be written")
	s.Contains(files, "schema_post_testdb.sql", "post-data schema DDL should be written")
	s.Contains(files, "testdb__users.sql", "table data should be written")
	s.Contains(files, "metadata.json", "metadata should be persisted")

	// Pre-data DDL contains the table definition.
	s.Contains(s.readFile(files["schema_pre_testdb.sql"]), "CREATE TABLE")

	// Data file carries the original row values.
	data := s.readFile(files["testdb__users.sql"])
	for _, name := range []string{"alice", "bob", "carol"} {
		s.Contains(data, name, "raw data should contain row value %q", name)
	}

	// Metadata decodes and references both data and schema dumps.
	var meta core.Metadata
	s.Require().NoError(json.Unmarshal([]byte(s.readFile(files["metadata.json"])), &meta))
	s.Equal(core.DBMSEngineMySQL, meta.Engine)
	s.Require().NotNil(meta.DataDump, "metadata should record the data dump")
	s.Require().NotNil(meta.SchemaDump, "metadata should record the schema dump")
	s.NotEmpty(meta.SchemaDump.DumpedDatabaseSchema, "schema dump stats should be present")
}

// TestFullTransformedDump runs RunDump with a Replace transformer on the name
// column and verifies the dumped data is transformed (original values gone).
func (s *pipelineEngineSuite) TestFullTransformedDump() {
	ctx := context.Background()
	down := s.seedUsers(ctx)
	defer down()

	storageDir := s.T().TempDir()
	cfg := s.baseConfig(ctx, storageDir)
	cfg.Dump.Transformation = config.TransformationConfig{
		{
			Schema: "testdb",
			Name:   "users",
			Transformers: config.Transformers{
				{
					Name: "Replace",
					Params: config.StaticParameters{
						"column": config.ParamsValue("name"),
						"value":  config.ParamsValue("REDACTED"),
					},
				},
			},
		},
	}

	_, err := NewDumpPipeline().RunDump(ctx, cfg)
	s.Require().NoError(err, "transformed RunDump should succeed end-to-end")

	files := s.storageFiles(storageDir)
	s.Require().Contains(files, "testdb__users.sql")
	data := s.readFile(files["testdb__users.sql"])

	s.Contains(data, "REDACTED", "transformed data should contain the replacement value")
	for _, name := range []string{"alice", "bob", "carol"} {
		s.NotContains(data, name, "original value %q should be transformed away", name)
	}
}

// storageFiles walks the directory-backed storage root and returns a map of
// base file name to absolute path.
func (s *pipelineEngineSuite) storageFiles(storageDir string) map[string]string {
	files := make(map[string]string)
	err := filepath.WalkDir(storageDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files[d.Name()] = path
		}
		return nil
	})
	s.Require().NoError(err)
	return files
}

func (s *pipelineEngineSuite) readFile(path string) string {
	b, err := os.ReadFile(path)
	s.Require().NoErrorf(err, "read %s", path)
	return string(b)
}
