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

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestStorage(t *testing.T) {
	ctx := context.Background()

	// Start Minio container
	minioContainer, err := minio.Run(ctx, "minio/minio:latest")
	require.NoError(t, err)
	defer func() {
		err := minioContainer.Terminate(ctx)
		assert.NoError(t, err)
	}()

	host, err := minioContainer.Host(ctx)
	require.NoError(t, err)
	port, err := minioContainer.MappedPort(ctx, "9000")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	bucketName := "test-bucket"
	cfg := S3Config{
		Bucket:          bucketName,
		Region:          "us-east-1",
		Endpoint:        endpoint,
		AccessKeyId:     minioContainer.Username,
		SecretAccessKey: minioContainer.Password,
		ForcePathStyle:  true,
		NoVerifySsl:     true,
	}

	storage, err := New(ctx, cfg, "debug")
	require.NoError(t, err)

	// Create bucket
	_, err = storage.service.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
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
		assert.ElementsMatch(t, []string{"test-file.txt", "stat-file.txt"}, files)
		assert.Len(t, dirs, 2)

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

		files, _, err := storage.SubStorage("subdir", true).ListDir(ctx)
		assert.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("GetCwd and Dirname", func(t *testing.T) {
		assert.Equal(t, "", storage.GetCwd())
		assert.Equal(t, ".", storage.Dirname())

		sub := storage.SubStorage("mysubdir", true)
		assert.Equal(t, "mysubdir/", sub.GetCwd())
		assert.Equal(t, "mysubdir", sub.Dirname())
	})

	t.Run("Ping", func(t *testing.T) {
		err := storage.Ping(ctx)
		// Ping calls Exists(s.Dirname()) which is Exists(".")
		// हेड-ऑब्जेक्ट on "." might fail or return Not Found on S3 depending on how it's handled.
		// In Minio/S3, objects are keys. There is no actual directory.
		// HeadObject on "." usually returns 404.
		// Let's see what happens.
		assert.NoError(t, err)

		sub := storage.SubStorage("mysubdir", true)
		err = sub.Ping(ctx)
		// Ping calls Exists("mysubdir")
		assert.NoError(t, err)
	})
}
