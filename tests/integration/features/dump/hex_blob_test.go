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

		// Read the dumped file from in-memory storage.
		rc, err := st.GetObject(ctx, "testdb__hex_blob_test.sql")
		s.Require().NoError(err)
		defer rc.Close()

		content, err := io.ReadAll(rc)
		s.Require().NoError(err)
		s.Require().NotEmpty(content)

		lines := splitNonEmpty(string(content))
		s.Require().Len(lines, 3, "expected 3 data rows")

		// Every binary column value (positions 1-6, 0-indexed after the id) must be
		// an X'...' hex literal — no raw bytes or escaped string literals.
		for i, line := range lines {
			cols := parseTupleCols(line)
			s.Require().Len(cols, 7, "row %d: expected 7 columns", i+1)
			// col_binary (BINARY(16)) — previously produced raw escaped bytes
			s.assertHexLiteral(cols[1], "row %d col_binary", i+1)
			// col_varbinary (VARBINARY(255)) — previously produced raw escaped bytes
			s.assertHexLiteral(cols[2], "row %d col_varbinary", i+1)
			// blob family — these already worked before the fix
			s.assertHexLiteral(cols[3], "row %d col_tinyblob", i+1)
			s.assertHexLiteral(cols[4], "row %d col_blob", i+1)
			s.assertHexLiteral(cols[5], "row %d col_medblob", i+1)
			s.assertHexLiteral(cols[6], "row %d col_longblob", i+1)
		}

		// Verify exact hex values for the known test rows.
		row1 := parseTupleCols(lines[0])
		s.Equal("X'00000000000000000000000000000000'", row1[1])
		s.Equal("X'0001020304'", row1[2])
		s.Equal("X'00'", row1[3])
		s.Equal("X'DEADBEEF'", row1[4])
		s.Equal("X'CAFEBABE0102030405'", row1[5])
		s.Equal("X'FFFEFDFCFBFA'", row1[6])

		row2 := parseTupleCols(lines[1])
		s.Equal("X'DEADBEEFDEADBEEFDEADBEEFDEADBEEF'", row2[1])
		s.Equal("X'80818283848586878889'", row2[2])

		row3 := parseTupleCols(lines[2])
		// \n \r \ ' — bytes that require escaping in a plain string literal
		s.Equal("X'0A0D5C27'", row3[2])
		s.Equal("X'1A'", row3[3]) // Ctrl-Z
		s.Equal("X'22'", row3[4]) // double-quote

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

// assertHexLiteral asserts that val is an X'...' hex literal.
func (s *hexBlobDumpSuite) assertHexLiteral(val, msgFmt string, args ...any) {
	s.Truef(
		strings.HasPrefix(val, "X'") && strings.HasSuffix(val, "'"),
		msgFmt+" = %q: expected X'...' hex literal", append(args, val)...,
	)
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

// parseTupleCols splits a dump tuple "(v1, v2, v3)" into its column values.
// It handles X'...' literals, 'string' literals, and unquoted values (NULL, numbers).
// This is a best-effort parser sufficient for the known test data shapes.
func parseTupleCols(line string) []string {
	// Strip outer parens.
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "(")
	line = strings.TrimSuffix(line, ")")

	var cols []string
	i := 0
	for i < len(line) {
		// Skip leading whitespace / comma separator.
		for i < len(line) && (line[i] == ' ' || line[i] == ',') {
			i++
		}
		if i >= len(line) {
			break
		}

		if line[i] == 'X' && i+1 < len(line) && line[i+1] == '\'' {
			// X'...' hex literal — find the closing single-quote.
			end := strings.Index(line[i+2:], "'")
			if end < 0 {
				break
			}
			cols = append(cols, line[i:i+2+end+1])
			i += 2 + end + 1
		} else if line[i] == '\'' {
			// 'string' literal — scan until unescaped closing quote.
			j := i + 1
			for j < len(line) {
				if line[j] == '\\' {
					j += 2
					continue
				}
				if line[j] == '\'' {
					break
				}
				j++
			}
			cols = append(cols, line[i:j+1])
			i = j + 1
		} else {
			// Unquoted token (NULL, number).
			end := strings.IndexAny(line[i:], ", ")
			if end < 0 {
				cols = append(cols, line[i:])
				break
			}
			cols = append(cols, line[i:i+end])
			i += end
		}
	}
	return cols
}

func TestHexBlobDumpSuite(t *testing.T) {
	suite.Run(t, new(hexBlobDumpSuite))
}
