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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// pipelineEngineSuite drives the v2 dump pipeline (NewDumpPipeline) against a
// live MySQL server. It verifies which pipeline stages currently function
// end-to-end and characterises the gaps that still block a full dump.
//
// Current status (verified by the tests below):
//   - Runtime + Discovery (session, introspection, dependency graph, subset)
//     work against a live server — see TestDiscovery.
//   - Context building is BLOCKED by an object-kind convention mismatch:
//     introspection (and the common graph/subset/filter stages) key tables on
//     core.ObjectKindTable ("table"), but the MySQL ExplicitDumpContextBuilder
//     reads/validates them as core.ObjectKindMysqlTable ("mysql.table") — see
//     TestContextBuildingObjectKindGap. This blocks planning and execution, so
//     the data path (storage provisioner -> DumpProcessor -> table factory ->
//     writer) cannot yet be exercised end-to-end through the pipeline.
type pipelineEngineSuite struct {
	testutils.MySQLContainerSuite
}

func (s *pipelineEngineSuite) SetupSuite() {
	s.SetImage("mysql:8")
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
// dependency-graph building, previous-metadata load and subset building. These
// stages all share the generic core.ObjectKindTable convention and work today.
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

	// The seeded users table is introspected under the generic table kind.
	tables := state.Discovery.Introspection.KindsMap[core.ObjectKindTable]
	var found bool
	for _, t := range tables {
		if t.Name == "users" {
			found = true
		}
	}
	s.True(found, "introspection should discover the users table under ObjectKindTable")
}

// TestContextBuildingObjectKindGap characterises the bug that currently blocks
// planning and execution: introspection populates KindsMap under
// core.ObjectKindTable, but the MySQL ExplicitDumpContextBuilder reads and
// validates tables as core.ObjectKindMysqlTable, so building the dump context
// fails with an "unsupported object kind" error.
//
// When the builder is reconciled to the generic table kind, this test should be
// updated to assert success (and the data-execution path can then be verified
// end-to-end).
func (s *pipelineEngineSuite) TestContextBuildingObjectKindGap() {
	ctx := context.Background()
	down := s.seedUsers(ctx)
	defer down()

	cfg := s.baseConfig(ctx, s.T().TempDir())
	p := NewDumpPipeline()

	_, err := p.RunValidateConfig(ctx, cfg)
	s.Require().Error(err, "context building is currently blocked by the object-kind mismatch")
	s.ErrorIs(err, errUnsupportedObjectKind)
}
