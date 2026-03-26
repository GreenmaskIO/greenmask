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
	"io"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

const mysqlImage = "mysql:8.4"

var (
	migrationUpTable = []string{
		`CREATE TABLE test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);`,
		`CREATE TABLE excluded_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);`,
		`CREATE TABLE data_excluded_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);`,
		`CREATE DATABASE IF NOT EXISTS other_db;`,
		`CREATE TABLE other_db.other_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);`,
	}
	migrationUpData = []string{
		`INSERT INTO test_table (name) VALUES ('test1'), ('test2');`,
		`INSERT INTO excluded_table (name) VALUES ('ex1');`,
		`INSERT INTO data_excluded_table (name) VALUES ('dex1');`,
		`INSERT INTO other_db.other_table (name) VALUES ('other1');`,
	}
	migrationDown = []string{
		`DROP TABLE test_table;`,
		`DROP TABLE excluded_table;`,
		`DROP TABLE data_excluded_table;`,
		`DROP DATABASE IF EXISTS other_db;`,
	}
)

type dumpTestSuite struct {
	testutils.MySQLContainerSuite
}

func (s *dumpTestSuite) SetupSuite() {
	s.MySQLContainerSuite.SetImage(mysqlImage).
		SetMigrationUp(append(migrationUpTable, migrationUpData...)).
		SetMigrationDown(migrationDown).
		SetupSuite()
}

func (s *dumpTestSuite) SetupContext(ctx context.Context, cfg *config.Config) context.Context {
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(commonmodels.MetaKeyEngine, "mysql")
	ctx = validationcollector.WithCollector(ctx, vc)
	return ctx
}

func (s *dumpTestSuite) SetupInfrastructure(cfg *config.Config) error {
	if err := utils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return err
	}
	return nil
}

func (s *dumpTestSuite) setupInfrastructure(ctx context.Context) context.Context {
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(commonmodels.MetaKeyEngine, "mysql")
	ctx = validationcollector.WithCollector(ctx, vc)
	s.Require().NoError(utils.SetDefaultContextLogger("debug", "text"))
	return ctx
}

func (s *dumpTestSuite) getBaseConfig(ctx context.Context) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = "mysql"
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	connOpts := s.GetRootConnectionOpts(ctx)
	cfg.Dump.MysqlConfig.Host = connOpts.Host
	cfg.Dump.MysqlConfig.Port = connOpts.Port
	cfg.Dump.MysqlConfig.User = connOpts.User
	cfg.Dump.MysqlConfig.Password = connOpts.Password
	cfg.Dump.MysqlConfig.ConnectDatabase = "testdb" // from pkg/testutils/pg.go testContainerDatabase
	cfg.Dump.Options.DataOnly = false
	cfg.Dump.Options.SchemaOnly = false
	cfg.Dump.Options.IncludeTable = nil
	cfg.Dump.Options.ExcludeTable = nil
	cfg.Dump.Options.IncludeSchema = nil
	cfg.Dump.Options.ExcludeSchema = nil
	cfg.Dump.Options.IncludeDatabase = nil
	cfg.Dump.Options.ExcludeDatabase = nil
	cfg.Dump.Options.ExcludeTableData = nil
	return cfg
}

func (s *dumpTestSuite) runDump(ctx context.Context, cfg *config.Config, st *validate.Storage,
	expectedParams any, expectedEnvs any,
) (*CmdProducerMock, *CmdRunnerMock) {
	if expectedParams == nil {
		expectedParams = mock.Anything
	}
	if expectedEnvs == nil {
		expectedEnvs = mock.Anything
	}
	cmdRunner := &CmdRunnerMock{}
	cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			w := args.Get(1).(io.Writer)
			_, err := w.Write([]byte("mock content"))
			s.Require().NoError(err)
		})

	cmdProducer := &CmdProducerMock{}
	cmdProducer.On("Produce", "mysqldump", expectedParams, expectedEnvs, mock.Anything).
		Return(cmdRunner, nil)

	dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
	s.Require().NoError(err)

	err = dumpProcess.Run(ctx)
	s.Require().NoError(err)

	return cmdProducer, cmdRunner
}

func (s *dumpTestSuite) requireOnlyFiles(files []string, expected ...string) {
	// Check for unexpected extra files first to provide a more specific error message
	expectedMap := make(map[string]struct{})
	for _, e := range expected {
		expectedMap[e] = struct{}{}
	}
	var unexpected []string
	for _, f := range files {
		if _, ok := expectedMap[f]; !ok {
			unexpected = append(unexpected, f)
		}
	}

	s.Require().Empty(unexpected, "Found unexpected files in storage: %v", unexpected)
	s.Require().ElementsMatch(expected, files, "Storage contains different set of files than expected.")
}

func (s *dumpTestSuite) TestDump() {
	ctx := context.Background()

	s.Run("common", func() {
		// This test case verify a common dump pipeline without filtering parameters, etc.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb", "other_db"}
		st := validate.New("")
		defer st.Cleanup()

		cmdRunner := &CmdRunnerMock{}
		cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				w := args.Get(1).(io.Writer)
				_, err := w.Write([]byte("CREATE TABLE test_table (id INT, name VARCHAR(255));"))
				s.Require().NoError(err)
			})

		cmdProducer := &CmdProducerMock{}
		cmdProducer.On("Produce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(cmdRunner, nil)

		dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
		s.Require().NoError(err)

		err = dumpProcess.Run(ctx)
		s.Require().NoError(err)

		// Verify results in the storage
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql", "other_db__other_table.sql", "metadata.json", "heartbeat")

		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("schema-only", func() {
		// This test case verify schema-only dump pipeline
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.SchemaOnly = true
		cfg.Dump.Options.DataOnly = false
		cfg.Dump.Options.IncludeSchema = []string{"testdb", "other_db"}

		st := validate.New("")
		defer st.Cleanup()

		cmdRunner := &CmdRunnerMock{}
		cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				w := args.Get(1).(io.Writer)
				_, err := w.Write([]byte("CREATE TABLE test_table (id INT, name VARCHAR(255));"))
				s.Require().NoError(err)
			})

		cmdProducer := &CmdProducerMock{}
		cmdProducer.On("Produce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(cmdRunner, nil)

		dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
		s.Require().NoError(err)

		err = dumpProcess.Run(ctx)
		s.Require().NoError(err)

		// Verify results in the storage
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "metadata.json", "heartbeat")

		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("data-only", func() {
		// This test case verify data-only dump pipeline
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.SchemaOnly = false
		cfg.Dump.Options.DataOnly = true
		cfg.Dump.Options.IncludeSchema = []string{"testdb", "other_db"}

		st := validate.New("")
		defer st.Cleanup()

		cmdProducer := &CmdProducerMock{}

		dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
		s.Require().NoError(err)

		err = dumpProcess.Run(ctx)
		s.Require().NoError(err)

		// Verify results in the storage
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql", "other_db__other_table.sql", "metadata.json", "heartbeat")

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("table-include", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeTable = []string{"testdb.test_table"}
		st := validate.New("")
		defer st.Cleanup()
		expectedCliParams := []string{"testdb.test_table"}
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, expectedCliParams, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("table-exclude", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.ExcludeTable = []string{"testdb.excluded_table"}
		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__data_excluded_table.sql", "other_db__other_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("table-data-exclude", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.ExcludeTableData = []string{"testdb.data_excluded_table"}
		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		// data_excluded_table.sql should be missing (but it currently fails because ExcludeTableData is not implemented)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__excluded_table.sql", "other_db__other_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("schema-include", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("schema-exclude", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.ExcludeSchema = []string{"other_db"}
		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("database-include", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeDatabase = []string{"testdb"}
		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("database-exclude", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.ExcludeDatabase = []string{"other_db"}
		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)
		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql", "metadata.json", "heartbeat")
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("table-data-include", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		// Include ONLY test_table data
		cfg.Dump.Options.IncludeTableData = []string{"testdb.test_table"}

		st := validate.New("")
		defer st.Cleanup()
		cmdProducer, cmdRunner := s.runDump(ctx, cfg, st, nil, nil)

		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)

		// All schemas should be there, but only test_table should have a data file
		// excluded_table, data_excluded_table, and other_db.other_table should NOT have data files
		s.requireOnlyFiles(files, "schema.sql", "testdb__test_table.sql", "metadata.json", "heartbeat")
		// Wait, all tables are discovered by introspector.
		// Introspector does NOT filter by IncludeTableData.
		// So schema.sql will contain all tables.
		// But in Storage, we check which files are present.
		// If IncludeTableData = ["testdb.test_table"], then only testdb__test_table.sql should be present.
		// Wait, what about other tables? They should NOT have data files.
		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})
}

func TestDumpSuite(t *testing.T) {
	suite.Run(t, new(dumpTestSuite))
}
