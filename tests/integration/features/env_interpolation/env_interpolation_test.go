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

// Package env_interpolation contains integration tests for the env-var
// interpolation feature in transformer parameters.
//
// Each test uses a real MySQL container for table-schema introspection and a
// CmdProducerMock to intercept mysqldump so that no actual dump file is
// produced. The transformer parameter values are expanded (or not) according
// to the resolve_env flag on the transformer config, and the tests verify that
// dump.Run succeeds or fails as expected.
package env_interpolation

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	mysqldump "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

const mysqlImage = "mysql:8.4"

var migrationUp = []string{
	`CREATE TABLE test_table (
		id   INT PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(255) NOT NULL
	)`,
	`INSERT INTO test_table (name) VALUES ('alice'), ('bob')`,
}

type EnvInterpolationSuite struct {
	testutils.MySQLContainerSuite
}

func (s *EnvInterpolationSuite) SetupSuite() {
	s.MySQLContainerSuite.
		SetImage(mysqlImage).
		SetMigrationUp(migrationUp).
		SetupSuite()
}

// setupCtx wires a validation collector and logger into ctx.
func (s *EnvInterpolationSuite) setupCtx(ctx context.Context, cfg *config.Config) context.Context {
	s.Require().NoError(utils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format))
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(commonmodels.MetaKeyEngine, "mysql")
	return validationcollector.WithCollector(ctx, vc)
}

// baseConfig returns a config pointing at the test container with sensible defaults.
func (s *EnvInterpolationSuite) baseConfig(ctx context.Context) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = commonmodels.DBMSEngineMySQL
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	opts := s.GetRootConnectionOpts(ctx)
	cfg.Dump.MysqlConfig.Host = opts.Host
	cfg.Dump.MysqlConfig.Port = opts.Port
	cfg.Dump.MysqlConfig.User = opts.User
	cfg.Dump.MysqlConfig.Password = opts.Password
	cfg.Dump.MysqlConfig.ConnectDatabase = "testdb"
	cfg.Dump.Options.IncludeSchema = []string{"testdb"}
	cfg.Dump.Options.Compress = false
	cfg.Dump.Options.Pgzip = false
	return cfg
}

// regexpReplaceTransformer builds a TransformationConfig for test_table using
// RegexpReplace. The replace value and resolve_env flag are provided by the caller.
func regexpReplaceTransformer(replaceValue string, resolveEnv bool) config.TransformationConfig {
	return config.TransformationConfig{
		{
			Schema: "testdb",
			Name:   "test_table",
			Transformers: config.Transformers{
				{
					Name:       "RegexpReplace",
					ResolveEnv: resolveEnv,
					Params: config.StaticParameters{
						"column":  config.ParamsValue("name"),
						"regexp":  config.ParamsValue(".*"),
						"replace": config.ParamsValue(replaceValue),
					},
				},
			},
		},
	}
}

// runDump wires a CmdProducerMock and runs the dump, returning any error from Run.
func (s *EnvInterpolationSuite) runDump(ctx context.Context, cfg *config.Config) error {
	cmdRunner := &CmdRunnerMock{}
	cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			w := args.Get(1).(io.Writer)
			_, _ = w.Write([]byte("-- mock mysqldump output"))
		})

	cmdProducer := &CmdProducerMock{}
	cmdProducer.On("Produce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(cmdRunner, nil)

	dumpProcess, err := mysqldump.NewDump(
		cfg,
		registry.DefaultTransformerRegistry,
		validate.New(""),
		cmdProducer,
		mysqldump.GetMySQLDumpOpts(cfg)...,
	)
	s.Require().NoError(err)
	return dumpProcess.Run(ctx)
}

// TestResolveEnv_VarSet verifies that a set env var is expanded into the
// transformer parameter when resolve_env is true.
func (s *EnvInterpolationSuite) TestResolveEnv_VarSet() {
	s.T().Setenv("GM_INT_TEST_REPLACE", "replaced_value")

	ctx := context.Background()
	cfg := s.baseConfig(ctx)
	ctx = s.setupCtx(ctx, cfg)
	cfg.Dump.Transformation = regexpReplaceTransformer("${GM_INT_TEST_REPLACE:-default_replace}", true)

	err := s.runDump(ctx, cfg)
	s.Require().NoError(err, "dump should succeed when env var is set and resolve_env=true")
}

// TestResolveEnv_VarUnset_DefaultUsed verifies that the default value in
// ${VAR:-default} is used when the variable is not set and resolve_env is true.
func (s *EnvInterpolationSuite) TestResolveEnv_VarUnset_DefaultUsed() {
	ctx := context.Background()
	cfg := s.baseConfig(ctx)
	ctx = s.setupCtx(ctx, cfg)
	cfg.Dump.Transformation = regexpReplaceTransformer("${GM_INT_UNSET_REPLACE:-default_replace}", true)

	err := s.runDump(ctx, cfg)
	s.Require().NoError(err, "dump should succeed when env var is unset and a default is provided")
}

// TestResolveEnv_Disabled verifies that ${...} syntax is left as a literal
// string when resolve_env is false (the default). The literal string is a valid
// replacement value for RegexpReplace, so the dump must succeed.
func (s *EnvInterpolationSuite) TestResolveEnv_Disabled() {
	ctx := context.Background()
	cfg := s.baseConfig(ctx)
	ctx = s.setupCtx(ctx, cfg)
	cfg.Dump.Transformation = regexpReplaceTransformer("${GM_INT_UNSET_REPLACE:-default_replace}", false)

	err := s.runDump(ctx, cfg)
	s.Require().NoError(err, "dump should succeed when resolve_env=false; ${...} is used as a literal replace value")
}

// TestResolveEnv_RequiredVarMissing verifies that ${VAR?message} causes the
// dump to fail with a fatal error when the variable is not set and resolve_env is true.
func (s *EnvInterpolationSuite) TestResolveEnv_RequiredVarMissing() {
	ctx := context.Background()
	cfg := s.baseConfig(ctx)
	ctx = s.setupCtx(ctx, cfg)
	cfg.Dump.Transformation = regexpReplaceTransformer(
		"${GM_INT_REQUIRED_REPLACE?replacement value is required}",
		true,
	)

	err := s.runDump(ctx, cfg)
	s.Require().Error(err, "dump should fail when a required env var is unset")

	vc := validationcollector.FromContext(ctx)
	s.Require().True(vc.IsFatal(), "validation collector should be in fatal state")
	warnings := vc.GetWarnings()
	s.Require().NotEmpty(warnings)

	var found bool
	for _, w := range warnings {
		if w.Meta != nil {
			if errStr, ok := w.Meta["Error"].(string); ok {
				if strings.Contains(errStr, "replacement value is required") {
					found = true
					break
				}
			}
		}
	}
	s.True(found, "validation warning should contain the ?message text from the env var reference")
}

func TestEnvInterpolationSuite(t *testing.T) {
	suite.Run(t, new(EnvInterpolationSuite))
}
