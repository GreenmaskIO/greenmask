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

package directory

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "directory_storage_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	storage, err := New(DirectoryConfig{Path: tmpDir})
	require.NoError(t, err)

	t.Run("Put and Get Object", func(t *testing.T) {
		key := "test-file.txt"
		content := []byte("hello world")
		err := storage.PutObject(ctx, key, bytes.NewReader(content))
		assert.NoError(t, err)

		reader, err := storage.GetObject(ctx, key)
		assert.NoError(t, err)
		defer reader.Close()

		res, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, content, res)
	})

	t.Run("Exists and Stat", func(t *testing.T) {
		key := "stat-file.txt"
		content := []byte("stat test")
		err := storage.PutObject(ctx, key, bytes.NewReader(content))
		assert.NoError(t, err)

		exists, err := storage.Exists(ctx, key)
		assert.NoError(t, err)
		assert.True(t, exists)

		stat, err := storage.Stat(key)
		assert.NoError(t, err)
		assert.True(t, stat.Exist)
		assert.Contains(t, stat.Name, key)

		exists, err = storage.Exists(ctx, "non-existent.txt")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("ListDir", func(t *testing.T) {
		err := storage.PutObject(ctx, "dir1/file1.txt", bytes.NewReader([]byte("1")))
		assert.NoError(t, err)
		err = storage.PutObject(ctx, "dir1/file2.txt", bytes.NewReader([]byte("2")))
		assert.NoError(t, err)
		err = storage.PutObject(ctx, "dir2/file3.txt", bytes.NewReader([]byte("3")))
		assert.NoError(t, err)

		files, dirs, err := storage.ListDir(ctx)
		assert.NoError(t, err)
		// Files created in previous tests
		assert.Contains(t, files, "test-file.txt")
		assert.Contains(t, files, "stat-file.txt")

		dirNames := make([]string, 0, len(dirs))
		for _, d := range dirs {
			dirNames = append(dirNames, d.Dirname())
		}
		assert.ElementsMatch(t, []string{"dir1", "dir2"}, dirNames)

		subStorage := storage.SubStorage("dir1", true)
		files, dirs, err = subStorage.ListDir(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"file1.txt", "file2.txt"}, files)
		assert.Empty(t, dirs)
	})

	t.Run("Delete", func(t *testing.T) {
		key := "delete-me.txt"
		err := storage.PutObject(ctx, key, bytes.NewReader([]byte("bye")))
		assert.NoError(t, err)

		err = storage.Delete(ctx, key)
		assert.NoError(t, err)

		exists, err := storage.Exists(ctx, key)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("DeleteAll", func(t *testing.T) {
		err := storage.PutObject(ctx, "subdir/f1.txt", bytes.NewReader([]byte("f1")))
		assert.NoError(t, err)
		err = storage.PutObject(ctx, "subdir/f2.txt", bytes.NewReader([]byte("f2")))
		assert.NoError(t, err)

		err = storage.DeleteAll(ctx, "subdir")
		assert.NoError(t, err)

		exists, err := storage.Exists(ctx, "subdir/f1.txt")
		assert.NoError(t, err)
		assert.False(t, exists)

		exists, err = storage.Exists(ctx, "subdir")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("GetCwd and Dirname", func(t *testing.T) {
		assert.Equal(t, tmpDir, storage.GetCwd())
		assert.Equal(t, path.Base(tmpDir), storage.Dirname())

		sub := storage.SubStorage("mysubdir", true)
		assert.Equal(t, path.Join(tmpDir, "mysubdir"), sub.GetCwd())
		assert.Equal(t, "mysubdir", sub.Dirname())
	})

	t.Run("Ping", func(t *testing.T) {
		err := storage.Ping(ctx)
		assert.NoError(t, err)

		sub := storage.SubStorage("non-existent-ping", true)
		err = sub.Ping(ctx)
		assert.Error(t, err)

		err = os.Mkdir(path.Join(tmpDir, "non-existent-ping"), 0755)
		assert.NoError(t, err)

		err = sub.Ping(ctx)
		assert.NoError(t, err)
	})
}
