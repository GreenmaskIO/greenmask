// Copyright 2023 Greenmask
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

package filestore

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
)

func TestDumpWholeDirectory(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), "a")
	writeFile(t, filepath.Join(src, "b.txt"), "bb")
	require.NoError(t, os.MkdirAll(filepath.Join(src, "nested"), 0o755))
	writeFile(t, filepath.Join(src, "nested", "c.txt"), "ccc")

	storage, dumpStorage := newStorage(t)

	cfg := &domains.FilestoreDump{
		Enabled:  true,
		RootPath: src,
	}
	require.NoError(t, Dump(ctx, cfg, dumpStorage, false))

	meta := readMetadata(t, storage, "filestore.json")
	require.Equal(t, 3, meta.TotalFiles)
	require.Len(t, meta.Archives, 1)

	files := readArchive(t, storage, meta.Archives[0].Name)
	require.Equal(t, map[string]string{
		"a.txt":        "a",
		"b.txt":        "bb",
		"nested/c.txt": "ccc",
	}, files)
}

func TestDumpSplitByMaxFiles(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), "a")
	writeFile(t, filepath.Join(src, "b.txt"), "b")

	storage, dumpStorage := newStorage(t)

	cfg := &domains.FilestoreDump{
		Enabled:  true,
		RootPath: src,
		Split: domains.FilestoreDumpSplit{
			MaxFiles: 1,
		},
	}
	require.NoError(t, Dump(ctx, cfg, dumpStorage, false))

	meta := readMetadata(t, storage, "filestore.json")
	require.Len(t, meta.Archives, 2)
	require.True(t, meta.TotalFiles == 2)
}

func TestRestoreExtractsArchives(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), "payload")

	storage, dumpStorage := newStorage(t)

	cfg := &domains.FilestoreDump{
		Enabled:  true,
		RootPath: src,
	}
	require.NoError(t, Dump(ctx, cfg, dumpStorage, false))

	target := t.TempDir()
	restoreCfg := &domains.FilestoreRestore{
		Enabled:    true,
		TargetPath: target,
	}
	require.NoError(t, Restore(ctx, restoreCfg, dumpStorage))

	bytes, err := os.ReadFile(filepath.Join(target, "a.txt"))
	require.NoError(t, err)
	require.Equal(t, "payload", string(bytes))
}

func newStorage(t *testing.T) (*directory.Storage, storages.Storager) {
	t.Helper()
	root := t.TempDir()
	base, err := directory.NewStorage(&directory.Config{Path: root})
	require.NoError(t, err)
	sub := base.SubStorage("dump", true)
	dirSub, ok := sub.(*directory.Storage)
	require.True(t, ok)
	return dirSub, sub
}

func readMetadata(t *testing.T, st *directory.Storage, name string) metadata {
	t.Helper()
	fullPath := filepath.Join(st.GetCwd(), "filestore", name)
	bytes, err := os.ReadFile(fullPath)
	require.NoError(t, err)
	var meta metadata
	require.NoError(t, json.Unmarshal(bytes, &meta))
	return meta
}

func readArchive(t *testing.T, st *directory.Storage, name string) map[string]string {
	t.Helper()
	fullPath := filepath.Join(st.GetCwd(), "filestore", name)
	file, err := os.Open(fullPath)
	require.NoError(t, err)
	defer file.Close()

	gz, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gz.Close()

	tr := tar.NewReader(gz)
	result := make(map[string]string)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data, err := io.ReadAll(tr)
		require.NoError(t, err)
		result[hdr.Name] = string(data)
	}
	return result
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))
}

