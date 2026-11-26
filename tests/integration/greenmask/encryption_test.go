// Copyright 2026 Greenmask
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
	"slices"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
)

const ageHeader = "age-encryption.org/v1"

// EncryptionSuite keeps plain and encrypted dumps in one storage and reads both.
type EncryptionSuite struct {
	suite.Suite
	tmpDir       string
	storageDir   string
	conn         *pgx.Conn
	dbConfig     *pgx.ConnConfig
	sourceDbName string
	targetDbs    []string
	key          string
	recipient    string
	plainDumpId  string
	encDumpId    string
}

func (s *EncryptionSuite) SetupSuite() {
	s.Require().NotEmpty(tempDir, "-tempDir non-empty flag required")
	s.Require().NotEmpty(pgBinPath, "-pgBinPath non-empty flag required")
	s.Require().NotEmpty(uri, "-uri non-empty flag required")
	s.Require().NotEmpty(greenmaskBinPath, "-greenmaskBinPath non-empty flag required")
	ctx := context.Background()

	var err error
	s.dbConfig, err = pgx.ParseConfig(uri)
	s.Require().NoError(err)
	s.tmpDir, err = os.MkdirTemp(tempDir, "encryption_test_")
	s.Require().NoError(err)
	s.storageDir = path.Join(s.tmpDir, "storage")
	s.Require().NoError(os.Mkdir(s.storageDir, 0700))

	id, err := age.GenerateX25519Identity()
	s.Require().NoError(err)
	s.key = id.String()
	s.recipient = id.Recipient().String()

	s.conn, err = pgx.ConnectConfig(ctx, s.dbConfig)
	s.Require().NoError(err)
	s.sourceDbName = fmt.Sprintf("enc_source_%d", time.Now().UnixMilli())
	_, err = s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", s.sourceDbName))
	s.Require().NoError(err)

	src := s.connectTo(s.sourceDbName)
	defer src.Close(ctx)
	_, err = src.Exec(ctx, `
		CREATE TABLE secrets (id int PRIMARY KEY, val text NOT NULL);
		INSERT INTO secrets SELECT g, 'value ' || g FROM generate_series(1, 1000) g;
	`)
	s.Require().NoError(err)
}

func (s *EncryptionSuite) TearDownSuite() {
	ctx := context.Background()
	if s.conn != nil {
		for _, db := range append(s.targetDbs, s.sourceDbName) {
			if db != "" {
				_, _ = s.conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db))
			}
		}
		_ = s.conn.Close(ctx)
	}
	if s.tmpDir != "" {
		_ = os.RemoveAll(s.tmpDir)
	}
}

func (s *EncryptionSuite) connectTo(db string) *pgx.Conn {
	cfg := s.dbConfig.Copy()
	cfg.Database = db
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	s.Require().NoError(err)
	return conn
}

func (s *EncryptionSuite) newTargetDb() string {
	name := fmt.Sprintf("enc_target_%d_%d", time.Now().UnixMilli(), len(s.targetDbs))
	_, err := s.conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", name))
	s.Require().NoError(err)
	s.targetDbs = append(s.targetDbs, name)
	return name
}

// withKey returns the env that sets the encryption key.
func withKey(key string) []string {
	return []string{fmt.Sprintf("STORAGE_ENCRYPTION_KEY=%s", key)}
}

// run runs greenmask against db and returns its combined output.
func (s *EncryptionSuite) run(db string, extraEnv []string, args ...string) (string, error) {
	cmd := exec.Command(path.Join(greenmaskBinPath, "greenmask"), args...)
	env := append(os.Environ(),
		fmt.Sprintf("PATH=%s:%s", pgBinPath, os.Getenv("PATH")),
		fmt.Sprintf("PGDATABASE=%s", db),
		fmt.Sprintf("PGHOST=%s", s.dbConfig.Host),
		fmt.Sprintf("PGPORT=%d", s.dbConfig.Port),
		fmt.Sprintf("PGUSER=%s", s.dbConfig.User),
		fmt.Sprintf("PGPASSWORD=%s", s.dbConfig.Password),
		"STORAGE_TYPE=directory",
		fmt.Sprintf("STORAGE_DIRECTORY_PATH=%s", s.storageDir),
		fmt.Sprintf("COMMON_PG_BIN_PATH=%s", pgBinPath),
		fmt.Sprintf("COMMON_TMP_DIR=%s", s.tmpDir),
	)
	if sslMode, ok := s.dbConfig.RuntimeParams["sslmode"]; ok {
		env = append(env, fmt.Sprintf("PGSSLMODE=%s", sslMode))
	} else {
		env = append(env, "PGSSLMODE=disable")
	}
	cmd.Env = append(env, extraEnv...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	fmt.Printf("greenmask %s:\n%s\n", strings.Join(args, " "), out.String())
	return out.String(), err
}

func (s *EncryptionSuite) dumpIds() []string {
	entries, err := os.ReadDir(s.storageDir)
	s.Require().NoError(err)
	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	return ids
}

// dump runs a dump and returns the id of the dump it created.
func (s *EncryptionSuite) dump(extraEnv []string) string {
	before := s.dumpIds()
	_, err := s.run(s.sourceDbName, extraEnv, "dump")
	s.Require().NoError(err, "dump failed")
	for _, id := range s.dumpIds() {
		if !slices.Contains(before, id) {
			return id
		}
	}
	s.FailNow("dump created no directory")
	return ""
}

func (s *EncryptionSuite) readFile(dumpId, name string) []byte {
	data, err := os.ReadFile(path.Join(s.storageDir, dumpId, name))
	s.Require().NoError(err)
	return data
}

func (s *EncryptionSuite) restoreAndCheck(dumpId string, extraEnv []string) {
	ctx := context.Background()
	target := s.newTargetDb()
	_, err := s.run(target, extraEnv, "restore", dumpId)
	s.Require().NoError(err, "restore failed")

	conn := s.connectTo(target)
	defer conn.Close(ctx)
	var count int
	var sum string
	err = conn.QueryRow(ctx, "SELECT count(*), md5(string_agg(val, ',' ORDER BY id)) FROM secrets").Scan(&count, &sum)
	s.Require().NoError(err)
	var wantSum string
	src := s.connectTo(s.sourceDbName)
	defer src.Close(ctx)
	s.Require().NoError(src.QueryRow(ctx, "SELECT md5(string_agg(val, ',' ORDER BY id)) FROM secrets").Scan(&wantSum))
	s.Equal(1000, count)
	s.Equal(wantSum, sum)
}

// listDumps runs list-dumps and returns its output.
func (s *EncryptionSuite) listDumps(extraEnv []string) string {
	out, err := s.run(s.dbConfig.Database, extraEnv, "list-dumps")
	s.Require().NoError(err, "list-dumps failed")
	return out
}

func lineWith(out, needle string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, needle) {
			return l
		}
	}
	return ""
}

func (s *EncryptionSuite) TestEncryption() {
	otherKey, err := age.GenerateX25519Identity()
	s.Require().NoError(err)

	s.Run("dump plain and encrypted into one storage", func() {
		s.plainDumpId = s.dump(nil)
		s.encDumpId = s.dump(withKey(s.key))
		s.Require().NotEqual(s.plainDumpId, s.encDumpId)
	})

	s.Run("storage layout", func() {
		var marker struct {
			Recipient string `json:"recipient"`
		}
		s.Require().NoError(json.Unmarshal(s.readFile(s.encDumpId, "encryption.json"), &marker))
		s.Equal(s.recipient, marker.Recipient)
		for _, name := range []string{"metadata.json", "toc.dat"} {
			s.True(bytes.HasPrefix(s.readFile(s.encDumpId, name), []byte(ageHeader)), name)
		}
		s.Equal("done", string(s.readFile(s.encDumpId, "heartbeat")), "the heartbeat stays plain")
		s.NoFileExists(path.Join(s.storageDir, s.plainDumpId, "encryption.json"))
		s.True(json.Valid(s.readFile(s.plainDumpId, "metadata.json")))
	})

	s.Run("list-dumps without key", func() {
		out := s.listDumps(nil)
		s.Contains(lineWith(out, s.plainDumpId), "done")
		encLine := lineWith(out, s.encDumpId)
		s.Contains(encLine, "done")
		s.Contains(encLine, "true")
		s.NotContains(encLine, s.sourceDbName, "the database needs the key")
	})

	s.Run("list-dumps with key", func() {
		out := s.listDumps(withKey(s.key))
		s.Contains(lineWith(out, s.plainDumpId), "done")
		s.Contains(lineWith(out, s.encDumpId), "done")
	})

	s.Run("restore plain dump with a key set", func() {
		s.restoreAndCheck(s.plainDumpId, withKey(s.key))
	})

	s.Run("show-dump plain dump with a key set", func() {
		out, err := s.run(s.dbConfig.Database, withKey(s.key), "show-dump", s.plainDumpId)
		s.Require().NoError(err)
		s.Contains(out, s.sourceDbName)
		s.Contains(out, "secrets")
	})

	s.Run("restore encrypted dump with key", func() {
		s.restoreAndCheck(s.encDumpId, withKey(s.key))
	})

	s.Run("restore encrypted dump without key", func() {
		// Without a key the storage is read as it is, so the ciphertext fails to parse.
		out, err := s.run(s.newTargetDb(), nil, "restore", s.encDumpId)
		s.Error(err)
		s.Contains(out, "cannot decode metadata")
	})

	s.Run("restore encrypted dump with another key", func() {
		out, err := s.run(s.newTargetDb(), withKey(otherKey.String()), "restore", s.encDumpId)
		s.Error(err)
		s.Contains(out, "another key")
		s.NotContains(out, otherKey.String())
	})

	s.Run("dump with an invalid key", func() {
		before := s.dumpIds()
		out, err := s.run(s.sourceDbName, withKey("AGE-SECRET-KEY-1GARBAGE"), "dump")
		s.Error(err)
		s.Contains(out, "invalid encryption key")
		s.Equal(before, s.dumpIds())
	})

	var latePlain string
	s.Run("validate --schema with a key reads the previous plain dump", func() {
		ctx := context.Background()
		latePlain = s.dump(nil)
		_, err := s.run(s.sourceDbName, withKey(s.key), "validate", "--schema")
		s.Require().NoError(err, "the schema has not changed")

		src := s.connectTo(s.sourceDbName)
		defer src.Close(ctx)
		_, err = src.Exec(ctx, "CREATE TABLE enc_extra (id int)")
		s.Require().NoError(err)
		out, err := s.run(s.sourceDbName, withKey(s.key), "validate", "--schema")
		s.Error(err)
		s.Contains(out, "Database schema has been changed")
		s.Contains(out, latePlain)
		_, err = src.Exec(ctx, "DROP TABLE enc_extra")
		s.Require().NoError(err)
	})

	s.Run("delete plain dump with a key set", func() {
		_, err := s.run(s.dbConfig.Database, withKey(s.key), "delete", latePlain)
		s.Require().NoError(err)
		s.NoDirExists(path.Join(s.storageDir, latePlain))
	})

	s.Run("delete encrypted dump without key", func() {
		_, err := s.run(s.dbConfig.Database, nil, "delete", s.encDumpId)
		s.Require().NoError(err)
		s.NoDirExists(path.Join(s.storageDir, s.encDumpId))
	})

	s.Run("retention without key deletes encrypted dumps", func() {
		oldEnc := s.dump(withKey(s.key))
		newest := s.dump(nil)
		_, err := s.run(s.dbConfig.Database, nil, "delete", "--retain-recent", "1")
		s.Require().NoError(err)
		s.NoDirExists(path.Join(s.storageDir, oldEnc))
		s.NoDirExists(path.Join(s.storageDir, s.plainDumpId))
		s.DirExists(path.Join(s.storageDir, newest))
	})
}
