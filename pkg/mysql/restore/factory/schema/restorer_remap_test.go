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

package schema

import (
	"context"
	"database/sql"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

// ── CmdProducer / CmdRunnerInterface mocks ─────────────────────────────────────

// capturingCmdProducer records the args passed to Produce so tests can assert
// the remapped database name appears there.
type capturingCmdProducer struct {
	lastArgs []string
}

func (c *capturingCmdProducer) Produce(_ string, args []string, _ []string, _ io.Reader) (utils.CmdRunnerInterface, error) {
	c.lastArgs = append([]string{}, args...)
	return &noopCmdRunner{}, nil
}

type noopCmdRunner struct{}

func (n *noopCmdRunner) ExecuteCmdAndForwardStdout(_ context.Context) error { return nil }
func (n *noopCmdRunner) ExecuteCmdAndWriteStdout(_ context.Context, _ io.Writer) error {
	return nil
}
func (n *noopCmdRunner) ExecuteCmd(_ context.Context, _ io.Writer, _ int) error { return nil }

// ── Storager mock ─────────────────────────────────────────────────────────────

type fakeStorager struct {
	content string
}

func (f *fakeStorager) GetObject(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(f.content)), nil
}
func (f *fakeStorager) GetCwd() string  { return "" }
func (f *fakeStorager) Dirname() string { return "" }
func (f *fakeStorager) ListDir(_ context.Context) ([]string, []core.Storager, error) {
	return nil, nil, nil
}
func (f *fakeStorager) PutObject(_ context.Context, _ string, _ io.Reader) error { return nil }
func (f *fakeStorager) Delete(_ context.Context, _ ...string) error              { return nil }
func (f *fakeStorager) DeleteAll(_ context.Context, _ string) error              { return nil }
func (f *fakeStorager) Exists(_ context.Context, _ string) (bool, error)         { return false, nil }
func (f *fakeStorager) SubStorage(_ string, _ bool) core.Storager                { return nil }
func (f *fakeStorager) Stat(_ string) (*core.StorageObjectStat, error)           { return nil, nil }
func (f *fakeStorager) Ping(_ context.Context) error                             { return nil }

// ── DatabaseSession / DB mocks ────────────────────────────────────────────────

type captureDB struct {
	queries []string
}

func (d *captureDB) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	d.queries = append(d.queries, query)
	return nil, nil
}
func (d *captureDB) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}
func (d *captureDB) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

type callbackSession struct {
	db *captureDB
}

func (s *callbackSession) Close(_ context.Context) error { return nil }
func (s *callbackSession) RunWithOperationalDB(ctx context.Context, fn func(context.Context, core.DB) error) error {
	return fn(ctx, s.db)
}
func (s *callbackSession) RunWithEngineResource(_ context.Context, _ func(context.Context, any) error) error {
	return core.ErrEngineResourceNotSupported
}

// ── restoreSchemaFile remap tests ──────────────────────────────────────────────

func TestMysqlSchemaRestorer_restoreSchemaFile_remapApplied(t *testing.T) {
	cmdProd := &capturingCmdProducer{}
	r := &MysqlSchemaRestorer{cmd: cmdProd}

	opts := SchemaRestoreOpts{
		RemapDatabase: map[string]string{"src": "dst"},
	}
	stat := core.SchemaDumpStat{
		DatabaseName: "src",
		FileName:     "schema_src.sql",
		Compression:  core.CompressionNone,
	}
	st := &fakeStorager{content: "-- schema"}

	err := r.restoreSchemaFile(context.Background(), []string{"--host=localhost"}, nil, opts, st, stat)
	require.NoError(t, err)

	// The remapped database name must be the final argument passed to mysql.
	require.NotEmpty(t, cmdProd.lastArgs)
	assert.Equal(t, "dst", cmdProd.lastArgs[len(cmdProd.lastArgs)-1])
}

func TestMysqlSchemaRestorer_restoreSchemaFile_noRemapKeepsOriginal(t *testing.T) {
	cmdProd := &capturingCmdProducer{}
	r := &MysqlSchemaRestorer{cmd: cmdProd}

	opts := SchemaRestoreOpts{
		RemapDatabase: map[string]string{"other": "x"},
	}
	stat := core.SchemaDumpStat{
		DatabaseName: "mydb",
		FileName:     "schema.sql",
		Compression:  core.CompressionNone,
	}
	st := &fakeStorager{content: "-- schema"}

	err := r.restoreSchemaFile(context.Background(), nil, nil, opts, st, stat)
	require.NoError(t, err)

	require.NotEmpty(t, cmdProd.lastArgs)
	assert.Equal(t, "mydb", cmdProd.lastArgs[len(cmdProd.lastArgs)-1])
}

func TestMysqlSchemaRestorer_restoreSchemaFile_emptyDatabaseNameNoArgAppended(t *testing.T) {
	cmdProd := &capturingCmdProducer{}
	r := &MysqlSchemaRestorer{cmd: cmdProd}

	opts := SchemaRestoreOpts{}
	stat := core.SchemaDumpStat{
		DatabaseName: "",
		FileName:     "schema.sql",
		Compression:  core.CompressionNone,
	}
	st := &fakeStorager{}

	err := r.restoreSchemaFile(context.Background(), []string{"--host=localhost"}, nil, opts, st, stat)
	require.NoError(t, err)

	// No extra arg for the database name when it is empty.
	assert.Equal(t, []string{"--host=localhost"}, cmdProd.lastArgs)
}

// ── createDatabases remap tests ────────────────────────────────────────────────

func TestMysqlSchemaRestorer_createDatabases_remapApplied(t *testing.T) {
	db := &captureDB{}
	session := &callbackSession{db: db}
	r := &MysqlSchemaRestorer{}

	opts := SchemaRestoreOpts{
		RemapDatabase: map[string]string{"src": "dst"},
	}

	err := r.createDatabases(context.Background(), session, []string{"src"}, opts)
	require.NoError(t, err)

	require.Len(t, db.queries, 1)
	assert.Contains(t, db.queries[0], "`dst`")
	assert.NotContains(t, db.queries[0], "`src`")
}

func TestMysqlSchemaRestorer_createDatabases_noRemapKeepsOriginal(t *testing.T) {
	db := &captureDB{}
	session := &callbackSession{db: db}
	r := &MysqlSchemaRestorer{}

	opts := SchemaRestoreOpts{
		RemapDatabase: map[string]string{"other": "x"},
	}

	err := r.createDatabases(context.Background(), session, []string{"mydb"}, opts)
	require.NoError(t, err)

	require.Len(t, db.queries, 1)
	assert.Contains(t, db.queries[0], "`mydb`")
}

func TestMysqlSchemaRestorer_createDatabases_ifNotExistsWithRemap(t *testing.T) {
	db := &captureDB{}
	session := &callbackSession{db: db}
	r := &MysqlSchemaRestorer{}

	opts := SchemaRestoreOpts{
		IfNotExists:   true,
		RemapDatabase: map[string]string{"src": "dst"},
	}

	err := r.createDatabases(context.Background(), session, []string{"src"}, opts)
	require.NoError(t, err)

	require.Len(t, db.queries, 1)
	assert.Contains(t, db.queries[0], "IF NOT EXISTS")
	assert.Contains(t, db.queries[0], "`dst`")
}

func TestMysqlSchemaRestorer_createDatabases_multipleDatabases(t *testing.T) {
	db := &captureDB{}
	session := &callbackSession{db: db}
	r := &MysqlSchemaRestorer{}

	opts := SchemaRestoreOpts{
		RemapDatabase: map[string]string{"a": "alpha"},
	}

	err := r.createDatabases(context.Background(), session, []string{"a", "b"}, opts)
	require.NoError(t, err)

	require.Len(t, db.queries, 2)
	assert.Contains(t, db.queries[0], "`alpha`")
	assert.Contains(t, db.queries[1], "`b`")
}
