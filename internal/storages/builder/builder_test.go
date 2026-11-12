// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package builder

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/greenmaskio/storages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/domains"
)

func TestGetStorage_directory_is_guarded(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := newStorageConfig()
	cfg.Type = DirectoryStorageType
	cfg.Directory.Path = root

	st, err := GetStorage(ctx, cfg, &domains.LogConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	dump, err := st.SubStorage("1700000000000", true)
	require.NoError(t, err)
	require.NoError(t, dump.PutObject(ctx, "metadata.json", bytes.NewBufferString("{}")))
	assert.FileExists(t, filepath.Join(root, "1700000000000", "metadata.json"))

	_, dirs, err := st.ListDir(ctx)
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	assert.Equal(t, "1700000000000", dirs[0].Dirname())

	_, err = st.SubStorage("../x", true)
	assert.ErrorIs(t, err, storages.ErrUnsafeKey)
	_, err = st.Exists(ctx, "../x/metadata.json")
	assert.ErrorIs(t, err, storages.ErrUnsafeKey)
	assert.ErrorIs(t, st.DeleteAll(ctx, ""), storages.ErrUnsafeKey)
}

func TestGetStorage_validation_errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(cfg *domains.StorageConfig)
		wantErr string
	}{
		{
			name:    "unknown type",
			mutate:  func(cfg *domains.StorageConfig) { cfg.Type = "ftp" },
			wantErr: "unknown storage type: 'ftp'",
		},
		{
			name:    "directory without path",
			mutate:  func(cfg *domains.StorageConfig) { cfg.Type = DirectoryStorageType },
			wantErr: "directory storage config validation failed: path is required",
		},
		{
			name: "s3 with unknown sse",
			mutate: func(cfg *domains.StorageConfig) {
				cfg.Type = S3StorageType
				cfg.S3.SSE = "kms"
			},
			wantErr: `s3 storage config validation failed: unknown sse "kms": must be one of AES256, aws:kms, aws:kms:dsse`,
		},
		{
			name: "s3 kms key without kms sse",
			mutate: func(cfg *domains.StorageConfig) {
				cfg.Type = S3StorageType
				cfg.S3.KMSKeyARN = "arn"
			},
			wantErr: "s3 storage config validation failed: kms_key_arn requires sse to be aws:kms or aws:kms:dsse",
		},
		{
			name:    "azure without container",
			mutate:  func(cfg *domains.StorageConfig) { cfg.Type = AzureStorageType },
			wantErr: "azure storage config validation failed: container is required",
		},
		{
			name:    "ssh without host",
			mutate:  func(cfg *domains.StorageConfig) { cfg.Type = SSHStorageType },
			wantErr: "ssh storage config validation failed: host is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newStorageConfig()
			tt.mutate(cfg)
			_, err := GetStorage(context.Background(), cfg, &domains.LogConfig{})
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
