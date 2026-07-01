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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/mocks"
)

// fakeConnAttrs satisfies the unexported connAttributes interface so the dumper
// can derive the mysqldump environment, connection params and vendor options
// from it at Dump time.
type fakeConnAttrs struct {
	env        []string
	envErr     error
	connParams []string
	vendorOpts []string
}

func (f fakeConnAttrs) MysqldumpEnv() ([]string, error)  { return f.env, f.envErr }
func (f fakeConnAttrs) MysqldumpConnParams() []string    { return f.connParams }
func (f fakeConnAttrs) MysqldumpVendorOptions() []string { return f.vendorOpts }

// fakeConn implements core.ConnectionConfigurer, returning whatever cfg is set
// (a fakeConnAttrs for happy paths, or something else to exercise the type
// assertion failure).
type fakeConn struct{ cfg any }

func (f fakeConn) ConnectionConfig() any { return f.cfg }

// dumpContent is the canned mysqldump stdout the stub provider writes into the
// pipe so the storage mock has something to drain and count.
const dumpContent = "-- MySQL dump\nCREATE TABLE t (id int);\n"

var _ core.VendorUtilityProvider = (*stubProvider)(nil)

// stubProvider is a core.VendorUtilityProvider test double. It records the args
// and env passed to Stream, optionally writes canned content into the writer
// (mirroring real mysqldump streaming), and returns a canned VendorUtility.
type stubProvider struct {
	content      string
	streamErr    error
	vendor       core.VendorUtility
	vendorErr    error
	streamArgs   []string
	streamEnv    []string
	streamCalled bool
	versionCalls int
}

func newStubProvider() *stubProvider {
	return &stubProvider{
		content: dumpContent,
		vendor:  core.VendorUtility{Name: "mysqldump", VersionString: "8.0.35", VersionParts: []string{"8", "0", "35"}},
	}
}

func (p *stubProvider) Name() string { return "mysqldump" }

func (p *stubProvider) Version(_ context.Context) (core.VendorUtility, error) {
	p.versionCalls++
	return p.vendor, p.vendorErr
}

func (p *stubProvider) Stream(_ context.Context, args, env []string, _ io.Reader, w io.Writer) error {
	p.streamCalled = true
	p.streamArgs = append([]string{}, args...)
	p.streamEnv = append([]string{}, env...)
	if p.streamErr != nil {
		return p.streamErr
	}
	if w != nil && p.content != "" {
		_, _ = w.Write([]byte(p.content))
	}
	return nil
}

func TestDumper_Dump_Parameters(t *testing.T) {
	connParams := []string{"--user", "root"}

	tests := []struct {
		name         string
		section      core.DumpSection
		scope        core.DumpScope
		vendorOpts   []string
		wantArgs     []string
		wantFileName string
	}{
		{
			name:    "pre-data without vendor options",
			section: core.DumpSectionPreData,
			wantArgs: []string{
				"--no-data", "--skip-triggers", "--skip-opt",
				"--user", "root",
				"shop",
			},
			wantFileName: "schema_pre_shop.sql",
		},
		{
			name:    "post-data without vendor options keeps triggers on by default",
			section: core.DumpSectionPostData,
			wantArgs: []string{
				"--no-create-info", "--no-data", "--no-create-db",
				"--user", "root",
				"--triggers",
				"shop",
			},
			wantFileName: "schema_post_shop.sql",
		},
		{
			name:       "pre-data drops post-data vendor flags, keeps generic ones",
			section:    core.DumpSectionPreData,
			vendorOpts: []string{"--single-transaction", "--routines"},
			wantArgs: []string{
				"--no-data", "--skip-triggers", "--skip-opt",
				"--user", "root",
				"--single-transaction",
				"shop",
			},
			wantFileName: "schema_pre_shop.sql",
		},
		{
			name:       "post-data handles trigger/routine/event flags explicitly",
			section:    core.DumpSectionPostData,
			vendorOpts: []string{"--skip-triggers", "--routines", "--events", "--add-drop-trigger", "--single-transaction"},
			wantArgs: []string{
				"--no-create-info", "--no-data", "--no-create-db",
				"--user", "root",
				"--routines", "--events", "--add-drop-trigger",
				"--single-transaction",
				"shop",
			},
			wantFileName: "schema_post_shop.sql",
		},
		{
			name:    "pre-data with table include/exclude filtering",
			section: core.DumpSectionPreData,
			scope: core.DumpScope{
				ExcludeTables: map[string][]string{"shop": {"audit"}},
				IncludeTables: map[string][]string{"shop": {"t1", "t2"}},
			},
			wantArgs: []string{
				"--no-data", "--skip-triggers", "--skip-opt",
				"--user", "root",
				"--ignore-table=shop.audit",
				"shop", "t1", "t2",
			},
			wantFileName: "schema_pre_shop.sql",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prov := newStubProvider()
			env := []string{"MYSQL_PWD=secret", "MYSQL_HOST=db"}

			storage := mocks.NewStorageMock()
			storage.On("PutObject", mock.Anything, tc.wantFileName, mock.Anything).Return(nil)

			d := &dumper{
				provider: prov,
				database: "shop",
				section:  tc.section,
				scope:    tc.scope,
			}
			conn := fakeConn{cfg: fakeConnAttrs{
				env:        env,
				connParams: connParams,
				vendorOpts: tc.vendorOpts,
			}}

			stat, err := d.Dump(context.Background(), conn, storage)
			require.NoError(t, err)

			assert.Equal(t, "shop", stat.DatabaseName)
			assert.Equal(t, tc.section, stat.Section)
			assert.Equal(t, tc.wantFileName, stat.FileName)
			assert.Equal(t, core.CompressionNone, stat.Compression)

			// The provider was streamed the exact command line and environment.
			assert.Equal(t, tc.wantArgs, prov.streamArgs)
			assert.Equal(t, env, prov.streamEnv)

			// The vendor utility identity rides along on the stat.
			require.NotNil(t, stat.VendorUtility)
			assert.Equal(t, "mysqldump", stat.VendorUtility.Name)
			assert.Equal(t, "8.0.35", stat.VendorUtility.VersionString)
			assert.Equal(t, []string{"8", "0", "35"}, stat.VendorUtility.VersionParts)

			storage.AssertExpectations(t)
			// The drained content was streamed through to storage unchanged.
			assert.Equal(t, dumpContent, storage.Data.String())
		})
	}
}

func TestDumper_Dump_PlainStat(t *testing.T) {
	prov := newStubProvider()
	storage := mocks.NewStorageMock()
	storage.On("PutObject", mock.Anything, "schema_pre_shop.sql", mock.Anything).Return(nil)

	d := &dumper{provider: prov, database: "shop", section: core.DumpSectionPreData}

	stat, err := d.Dump(context.Background(), fakeConn{cfg: fakeConnAttrs{}}, storage)
	require.NoError(t, err)

	assert.Equal(t, []string{"--no-data", "--skip-triggers", "--skip-opt", "shop"}, prov.streamArgs)
	assert.Equal(t, int64(len(dumpContent)), stat.OriginalSize)
	assert.Equal(t, int64(len(dumpContent)), stat.CompressedSize)
	assert.Equal(t, core.CompressionNone, stat.Compression)
}

func TestDumper_Dump_Compression(t *testing.T) {
	tests := []struct {
		name            string
		compression     core.Compression
		wantFileName    string
		wantCompression core.Compression
	}{
		{
			name:            "gzip",
			compression:     core.CompressionGzip,
			wantFileName:    "schema_pre_shop.sql.gz",
			wantCompression: core.CompressionGzip,
		},
		{
			name:            "pgzip",
			compression:     core.CompressionPgzip,
			wantFileName:    "schema_pre_shop.sql.gz",
			wantCompression: core.CompressionPgzip,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prov := newStubProvider()
			storage := mocks.NewStorageMock()
			storage.On("PutObject", mock.Anything, tc.wantFileName, mock.Anything).Return(nil)

			d := &dumper{
				provider:    prov,
				database:    "shop",
				section:     core.DumpSectionPreData,
				compression: tc.compression,
			}

			stat, err := d.Dump(context.Background(), fakeConn{cfg: fakeConnAttrs{}}, storage)
			require.NoError(t, err)

			assert.Equal(t, tc.wantFileName, stat.FileName)
			assert.Equal(t, tc.wantCompression, stat.Compression)
			// Compressed output is smaller-or-different than the raw input, but
			// both byte counts must be non-zero for a real gzip stream.
			assert.Positive(t, stat.OriginalSize)
			assert.Positive(t, stat.CompressedSize)

			storage.AssertExpectations(t)
		})
	}
}

func TestDumper_Dump_Errors(t *testing.T) {
	t.Run("unknown section", func(t *testing.T) {
		prov := newStubProvider()
		storage := mocks.NewStorageMock()
		d := &dumper{provider: prov, database: "shop", section: core.DumpSectionData}

		_, err := d.Dump(context.Background(), fakeConn{cfg: fakeConnAttrs{}}, storage)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown schema section")
		// Stream/PutObject must not be reached for an unknown section.
		assert.False(t, prov.streamCalled)
		storage.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("environment build failure", func(t *testing.T) {
		prov := newStubProvider()
		storage := mocks.NewStorageMock()
		d := &dumper{provider: prov, database: "shop", section: core.DumpSectionPreData}
		conn := fakeConn{cfg: fakeConnAttrs{envErr: errors.New("boom")}}

		_, err := d.Dump(context.Background(), conn, storage)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "build mysqldump environment")
		assert.False(t, prov.streamCalled)
	})

	t.Run("connection config without mysqldump attributes", func(t *testing.T) {
		prov := newStubProvider()
		storage := mocks.NewStorageMock()
		d := &dumper{provider: prov, database: "shop", section: core.DumpSectionPreData}
		conn := fakeConn{cfg: "not-conn-attributes"}

		_, err := d.Dump(context.Background(), conn, storage)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not provide mysqldump attributes")
		assert.False(t, prov.streamCalled)
	})

	t.Run("version probe failure does not fail the dump", func(t *testing.T) {
		prov := newStubProvider()
		prov.vendorErr = errors.New("mysqldump not found")
		storage := mocks.NewStorageMock()
		storage.On("PutObject", mock.Anything, "schema_pre_shop.sql", mock.Anything).Return(nil)
		d := &dumper{provider: prov, database: "shop", section: core.DumpSectionPreData}

		stat, err := d.Dump(context.Background(), fakeConn{cfg: fakeConnAttrs{}}, storage)
		require.NoError(t, err)
		assert.Nil(t, stat.VendorUtility)
	})
}

// TestFactory_New_Wiring covers the Payload -> dumper field mapping through the
// public Factory.New entry point (database, section, compression, pgzip).
func TestFactory_New_Wiring(t *testing.T) {
	prov := newStubProvider()
	storage := mocks.NewStorageMock()
	storage.On("PutObject", mock.Anything, "schema_pre_shop.sql.gz", mock.Anything).Return(nil)

	f := NewFactory(prov)
	sd, err := f.New(core.SchemaDumpSpec{Payload: Payload{
		Name:        "shop",
		Section:     core.DumpSectionPreData,
		Compression: core.CompressionPgzip,
	}})
	require.NoError(t, err)

	stat, err := sd.Dump(context.Background(), fakeConn{cfg: fakeConnAttrs{}}, storage)
	require.NoError(t, err)

	assert.Equal(t, "schema_pre_shop.sql.gz", stat.FileName)
	assert.Equal(t, core.CompressionPgzip, stat.Compression)
	assert.Equal(t, core.DumpSectionPreData, stat.Section)

	storage.AssertExpectations(t)
}

func TestFactory_New_WrongPayload(t *testing.T) {
	f := NewFactory(newStubProvider())
	_, err := f.New(core.SchemaDumpSpec{Payload: "not-a-payload"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected schema.Payload")
}
