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
	"slices"

	"github.com/stretchr/testify/mock"

	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
)

// runSchemaDump sets up a capturing CmdProducerMock and runs the dump.
// Returns every []string args slice that mysqldump was invoked with, in call order.
func (s *dumpTestSuite) runSchemaDump(
	ctx context.Context,
	cfg *config.Config,
	st *validate.Storage,
) (capturedArgs [][]string, cmdProducer *CmdProducerMock) {
	cmdRunner := &CmdRunnerMock{}
	cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			w := args.Get(1).(io.Writer)
			_, err := w.Write([]byte("-- mock schema content"))
			s.Require().NoError(err)
		})

	cmdProducer = &CmdProducerMock{}
	cmdProducer.On("Produce", "mysqldump", mock.Anything, mock.Anything, mock.Anything).
		Return(cmdRunner, nil).
		Run(func(callArgs mock.Arguments) {
			capturedArgs = append(capturedArgs, callArgs.Get(1).([]string))
		})

	dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
	s.Require().NoError(err)
	s.Require().NoError(dumpProcess.Run(ctx))
	return capturedArgs, cmdProducer
}

// filterPreDataArgs returns calls where --skip-triggers is present but --no-create-info is not.
// Pre-data calls always begin with [--no-data, --skip-triggers, ...].
func filterPreDataArgs(capturedArgs [][]string) [][]string {
	var result [][]string
	for _, args := range capturedArgs {
		if slices.Contains(args, "--skip-triggers") && !slices.Contains(args, "--no-create-info") {
			result = append(result, args)
		}
	}
	return result
}

// filterPostDataArgs returns calls where --no-create-info is present.
// Post-data calls always begin with [--no-create-info, --no-data, --no-create-db, ...].
func filterPostDataArgs(capturedArgs [][]string) [][]string {
	var result [][]string
	for _, args := range capturedArgs {
		if slices.Contains(args, "--no-create-info") {
			result = append(result, args)
		}
	}
	return result
}

func (s *dumpTestSuite) TestSchemaSections() {
	ctx := context.Background()

	s.Run("pre-data-params", func() {
		// Pre-data mysqldump call must use --no-data and --skip-triggers, and must
		// not carry any post-data flags (--no-create-info, --routines, --events).
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		preDataCalls := filterPreDataArgs(capturedArgs)
		s.Require().Len(preDataCalls, 1, "expected exactly one pre-data mysqldump call for testdb")

		preArgs := preDataCalls[0]
		s.Assert().Equal("--no-data", preArgs[0])
		s.Assert().Equal("--skip-triggers", preArgs[1])
		s.Assert().True(slices.Contains(preArgs, "testdb"))
		s.Assert().False(slices.Contains(preArgs, "--no-create-info"), "pre-data must not carry --no-create-info")
		s.Assert().False(slices.Contains(preArgs, "--routines"), "pre-data must not carry --routines")
		s.Assert().False(slices.Contains(preArgs, "--events"), "pre-data must not carry --events")

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("post-data-params-default", func() {
		// Post-data mysqldump call must use --no-create-info, --no-data, --no-create-db,
		// and include --triggers by default (when no --skip-triggers vendor option is given).
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		postDataCalls := filterPostDataArgs(capturedArgs)
		s.Require().Len(postDataCalls, 1, "expected exactly one post-data mysqldump call for testdb")

		postArgs := postDataCalls[0]
		s.Assert().Equal("--no-create-info", postArgs[0])
		s.Assert().Equal("--no-data", postArgs[1])
		s.Assert().Equal("--no-create-db", postArgs[2])
		s.Assert().True(slices.Contains(postArgs, "--triggers"), "post-data must include --triggers by default")
		s.Assert().False(slices.Contains(postArgs, "--skip-triggers"), "post-data must not contain --skip-triggers")
		s.Assert().False(slices.Contains(postArgs, "--routines"), "post-data must not carry --routines unless requested")
		s.Assert().False(slices.Contains(postArgs, "--events"), "post-data must not carry --events unless requested")
		s.Assert().True(slices.Contains(postArgs, "testdb"))

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("skip-triggers-vendor-option", func() {
		// When the user sets --skip-triggers in VendorOptions, the post-data call must
		// omit --triggers entirely. The pre-data call always keeps its own --skip-triggers.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.MysqlConfig.VendorOptions = []string{"--skip-triggers"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		preDataCalls := filterPreDataArgs(capturedArgs)
		s.Require().Len(preDataCalls, 1)
		s.Assert().True(slices.Contains(preDataCalls[0], "--skip-triggers"),
			"pre-data always emits --skip-triggers")

		postDataCalls := filterPostDataArgs(capturedArgs)
		s.Require().Len(postDataCalls, 1)
		postArgs := postDataCalls[0]
		s.Assert().False(slices.Contains(postArgs, "--triggers"),
			"post-data must not include --triggers when --skip-triggers is set")
		s.Assert().False(slices.Contains(postArgs, "--skip-triggers"),
			"post-data must not forward --skip-triggers")

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("routines-and-events-vendor-options", func() {
		// --routines and --events are post-data flags: they must appear only in the
		// post-data call and must be absent from the pre-data call.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.MysqlConfig.VendorOptions = []string{"--routines", "--events"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		preDataCalls := filterPreDataArgs(capturedArgs)
		s.Require().Len(preDataCalls, 1)
		s.Assert().False(slices.Contains(preDataCalls[0], "--routines"),
			"pre-data must not carry --routines")
		s.Assert().False(slices.Contains(preDataCalls[0], "--events"),
			"pre-data must not carry --events")

		postDataCalls := filterPostDataArgs(capturedArgs)
		s.Require().Len(postDataCalls, 1)
		postArgs := postDataCalls[0]
		s.Assert().True(slices.Contains(postArgs, "--routines"),
			"post-data must include --routines when requested")
		s.Assert().True(slices.Contains(postArgs, "--events"),
			"post-data must include --events when requested")
		s.Assert().True(slices.Contains(postArgs, "--triggers"),
			"post-data must still include --triggers by default")

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("add-drop-trigger-vendor-option", func() {
		// --add-drop-trigger is a post-data flag and must not appear in pre-data.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.MysqlConfig.VendorOptions = []string{"--add-drop-trigger"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		preDataCalls := filterPreDataArgs(capturedArgs)
		s.Require().Len(preDataCalls, 1)
		s.Assert().False(slices.Contains(preDataCalls[0], "--add-drop-trigger"),
			"pre-data must not carry --add-drop-trigger")

		postDataCalls := filterPostDataArgs(capturedArgs)
		s.Require().Len(postDataCalls, 1)
		s.Assert().True(slices.Contains(postDataCalls[0], "--add-drop-trigger"),
			"post-data must include --add-drop-trigger when requested")

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("multi-db-each-db-gets-own-calls", func() {
		// Each allowed database must get its own pre-data and post-data mysqldump call.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb", "other_db"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		preDataCalls := filterPreDataArgs(capturedArgs)
		postDataCalls := filterPostDataArgs(capturedArgs)

		s.Require().Len(preDataCalls, 2, "expected one pre-data call per database")
		s.Require().Len(postDataCalls, 2, "expected one post-data call per database")

		dbsInPreData := collectDBArgs(preDataCalls)
		s.Assert().ElementsMatch([]string{"testdb", "other_db"}, dbsInPreData)

		dbsInPostData := collectDBArgs(postDataCalls)
		s.Assert().ElementsMatch([]string{"testdb", "other_db"}, dbsInPostData)

		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files,
			"schema_pre_testdb.sql", "schema_post_testdb.sql",
			"schema_pre_other_db.sql", "schema_post_other_db.sql",
			"testdb__test_table.sql", "testdb__excluded_table.sql", "testdb__data_excluded_table.sql",
			"other_db__other_table.sql",
			"metadata.json", "heartbeat",
		)

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("schema-only-produces-pre-and-post-files", func() {
		// schema-only mode must produce both pre-data and post-data schema files and no data files.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.Options.SchemaOnly = true
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		s.Require().Len(capturedArgs, 2, "schema-only must produce exactly 2 mysqldump calls (pre + post per DB)")

		files, _, err := st.ListDir(ctx)
		s.Require().NoError(err)
		s.requireOnlyFiles(files,
			"schema_pre_testdb.sql", "schema_post_testdb.sql",
			"metadata.json", "heartbeat",
		)

		cmdProducer.AssertExpectations(s.T())
	})

	s.Run("table-filter-applied-to-both-sections", func() {
		// When IncludeTable restricts which tables are dumped, both pre-data and post-data
		// calls must carry the same database positional argument.
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.Options.IncludeTable = []string{"testdb.test_table"}
		st := validate.New("")
		defer st.Cleanup()

		capturedArgs, cmdProducer := s.runSchemaDump(ctx, cfg, st)

		preDataCalls := filterPreDataArgs(capturedArgs)
		postDataCalls := filterPostDataArgs(capturedArgs)

		s.Require().Len(preDataCalls, 1)
		s.Require().Len(postDataCalls, 1)

		// Both calls must contain "testdb" and "test_table" as positional args.
		s.Assert().True(slices.Contains(preDataCalls[0], "testdb"))
		s.Assert().True(slices.Contains(preDataCalls[0], "test_table"))
		s.Assert().True(slices.Contains(postDataCalls[0], "testdb"))
		s.Assert().True(slices.Contains(postDataCalls[0], "test_table"))

		cmdProducer.AssertExpectations(s.T())
	})
}

// collectDBArgs extracts the positional database/table args from a set of captured arg slices.
// It identifies them as the first arg after the connection params block
// (i.e. args that are neither flags nor flag values).
func collectDBArgs(calls [][]string) []string {
	seen := make(map[string]bool)
	for _, args := range calls {
		skipNext := false
		for _, arg := range args {
			if skipNext {
				skipNext = false
				continue
			}
			if arg == "--user" || arg == "--host" || arg == "--port" {
				skipNext = true
				continue
			}
			if len(arg) > 0 && arg[0] == '-' {
				continue
			}
			// First non-flag arg is the database name.
			if !seen[arg] {
				seen[arg] = true
			}
			break
		}
	}
	result := make([]string, 0, len(seen))
	for db := range seen {
		result = append(result, db)
	}
	return result
}
