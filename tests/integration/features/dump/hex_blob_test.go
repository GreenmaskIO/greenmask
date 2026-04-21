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
	"bufio"
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
	"github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

var (
	hexBlobMigrationUp = []string{
		`CREATE TABLE hex_blob_test (
			id            INT AUTO_INCREMENT PRIMARY KEY,
			col_binary    BINARY(16),
			col_varbinary VARBINARY(255),
			col_tinyblob  TINYBLOB,
			col_blob      BLOB,
			col_medblob   MEDIUMBLOB,
			col_longblob  LONGBLOB
		)`,
		// Row 1: null bytes and low control bytes (invisible without hex-blob)
		`INSERT INTO hex_blob_test (col_binary, col_varbinary, col_tinyblob, col_blob, col_medblob, col_longblob)
		 VALUES (
			UNHEX('00000000000000000000000000000000'),
			UNHEX('0001020304'),
			UNHEX('00'),
			UNHEX('DEADBEEF'),
			UNHEX('CAFEBABE0102030405'),
			UNHEX('FFFEFDFCFBFA')
		 )`,
		// Row 2: high bytes invalid under utf8mb4 — corrupt without hex-blob
		`INSERT INTO hex_blob_test (col_binary, col_varbinary, col_tinyblob, col_blob, col_medblob, col_longblob)
		 VALUES (
			UNHEX('DEADBEEFDEADBEEFDEADBEEFDEADBEEF'),
			UNHEX('80818283848586878889'),
			UNHEX('F0F1F2F3'),
			UNHEX('C0C1FEFF'),
			UNHEX('E0E1E2E3E4E5'),
			UNHEX('F8F9FAFBFCFDFEFF')
		 )`,
		// Row 3: bytes that need escaping in plain string literals
		`INSERT INTO hex_blob_test (col_binary, col_varbinary, col_tinyblob, col_blob, col_medblob, col_longblob)
		 VALUES (
			UNHEX('00000000000000000000000000000000'),
			UNHEX('0A0D5C27'),
			UNHEX('1A'),
			UNHEX('22'),
			UNHEX('0000000000'),
			UNHEX('AABBCCDDEE')
		 )`,
	}

	hexBlobMigrationDown = []string{
		`DROP TABLE hex_blob_test`,
	}
)

type hexBlobDumpSuite struct {
	testutils.MySQLContainerSuite
}

func (s *hexBlobDumpSuite) SetupSuite() {
	s.MySQLContainerSuite.SetImage(mysqlImage).
		SetMigrationUp(append(migrationUpTable, append(migrationUpData, hexBlobMigrationUp...)...)).
		SetMigrationDown(append(migrationDown, hexBlobMigrationDown...)).
		SetupSuite()
}

func (s *hexBlobDumpSuite) setupInfrastructure(ctx context.Context) context.Context {
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, "mysql").Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(commonmodels.MetaKeyEngine, "mysql")
	ctx = validationcollector.WithCollector(ctx, vc)
	s.Require().NoError(utils.SetDefaultContextLogger("debug", "text"))
	return ctx
}

func (s *hexBlobDumpSuite) getBaseConfig(ctx context.Context) *config.Config {
	cfg := config.NewConfig()
	cfg.Engine = "mysql"
	cfg.Log.Level = "debug"
	cfg.Log.Format = "text"

	connOpts := s.GetRootConnectionOpts(ctx)
	cfg.Dump.MysqlConfig.Host = connOpts.Host
	cfg.Dump.MysqlConfig.Port = connOpts.Port
	cfg.Dump.MysqlConfig.User = connOpts.User
	cfg.Dump.MysqlConfig.Password = connOpts.Password
	cfg.Dump.MysqlConfig.ConnectDatabase = "testdb"
	cfg.Dump.Options.DataOnly = false
	cfg.Dump.Options.SchemaOnly = false
	cfg.Dump.Options.IncludeTable = nil
	cfg.Dump.Options.ExcludeTable = nil
	cfg.Dump.Options.IncludeSchema = nil
	cfg.Dump.Options.ExcludeSchema = nil
	cfg.Dump.Options.IncludeDatabase = nil
	cfg.Dump.Options.ExcludeDatabase = nil
	cfg.Dump.Options.ExcludeTableData = nil
	cfg.Dump.Options.IncludeTableData = nil
	cfg.Dump.Options.IncludeTableDefinition = nil
	cfg.Dump.Options.ExcludeTableDefinition = nil
	cfg.Dump.MysqlConfig.VendorOptions = nil
	cfg.Dump.Options.Compress = false
	cfg.Dump.Options.Pgzip = false
	return cfg
}

func (s *hexBlobDumpSuite) TestHexBlobDump() {
	ctx := context.Background()

	const (
		hexRow1 = `('1', X'00000000000000000000000000000000', X'0001020304', X'00', X'DEADBEEF', X'CAFEBABE0102030405', X'FFFEFDFCFBFA')`
		hexRow2 = `('2', X'DEADBEEFDEADBEEFDEADBEEFDEADBEEF', X'80818283848586878889', X'F0F1F2F3', X'C0C1FEFF', X'E0E1E2E3E4E5', X'F8F9FAFBFCFDFEFF')`
		hexRow3 = `('3', X'00000000000000000000000000000000', X'0A0D5C27', X'1A', X'22', X'0000000000', X'AABBCCDDEE')`
	)

	s.Run("hex_blob_enabled_all_binary_cols_encoded", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.Options.IncludeTable = []string{"testdb.hex_blob_test"}
		cfg.Dump.MysqlConfig.HexBlob = true

		st := validate.New("")
		defer st.Cleanup()

		cmdRunner := &CmdRunnerMock{}
		cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				w := args.Get(1).(io.Writer)
				_, err := w.Write([]byte("-- mock schema"))
				s.Require().NoError(err)
			})
		cmdProducer := &CmdProducerMock{}
		cmdProducer.On("Produce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(cmdRunner, nil)

		dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
		s.Require().NoError(err)
		s.Require().NoError(dumpProcess.Run(ctx))

		rc, err := st.GetObject(ctx, "testdb__hex_blob_test.sql")
		s.Require().NoError(err)
		defer rc.Close()

		content, err := io.ReadAll(rc)
		s.Require().NoError(err)

		lines := splitNonEmpty(string(content))
		s.Require().Len(lines, 3, "expected 3 data rows")
		s.Equal(hexRow1, lines[0])
		s.Equal(hexRow2, lines[1])
		s.Equal(hexRow3, lines[2])

		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})

	s.Run("hex_blob_disabled_binary_cols_not_hex", func() {
		ctx := s.setupInfrastructure(ctx)
		cfg := s.getBaseConfig(ctx)
		cfg.Dump.Options.IncludeSchema = []string{"testdb"}
		cfg.Dump.Options.IncludeTable = []string{"testdb.hex_blob_test"}
		cfg.Dump.MysqlConfig.HexBlob = false

		st := validate.New("")
		defer st.Cleanup()

		cmdRunner := &CmdRunnerMock{}
		cmdRunner.On("ExecuteCmdAndWriteStdout", mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				w := args.Get(1).(io.Writer)
				_, err := w.Write([]byte("-- mock schema"))
				s.Require().NoError(err)
			})
		cmdProducer := &CmdProducerMock{}
		cmdProducer.On("Produce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(cmdRunner, nil)

		dumpProcess, err := dump.NewDump(cfg, registry.DefaultTransformerRegistry, st, cmdProducer, dump.GetMySQLDumpOpts(cfg)...)
		s.Require().NoError(err)
		s.Require().NoError(dumpProcess.Run(ctx))

		rc, err := st.GetObject(ctx, "testdb__hex_blob_test.sql")
		s.Require().NoError(err)
		defer rc.Close()

		content, err := io.ReadAll(rc)
		s.Require().NoError(err)
		s.Require().NotEmpty(content)

		lines := splitNonEmpty(string(content))
		s.Require().Len(lines, 3)

		// With hex-blob off, NO column should be an X'...' hex literal —
		// everything goes through the string/escape path.
		for i, line := range lines {
			s.NotContains(line, "X'", "row %d should not contain X' hex literals when hex-blob is disabled", i+1)
		}

		cmdProducer.AssertExpectations(s.T())
		cmdRunner.AssertExpectations(s.T())
	})
}

// splitNonEmpty splits content by newlines and returns non-empty lines.
func splitNonEmpty(content string) []string {
	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestHexBlobDumpSuite(t *testing.T) {
	suite.Run(t, new(hexBlobDumpSuite))
}
