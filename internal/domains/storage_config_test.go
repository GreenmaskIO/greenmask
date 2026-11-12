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

package domains

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3StorageConfig_Validate_sse_modes(t *testing.T) {
	for _, mode := range []string{"", "AES256", "aws:kms", "aws:kms:dsse"} {
		t.Run("accepts "+mode, func(t *testing.T) {
			require.NoError(t, (&S3StorageConfig{SSE: mode}).Validate())
		})
	}

	for _, mode := range []string{"aes256", "AWS:KMS", "kms", "none", "sse-c"} {
		t.Run("rejects "+mode, func(t *testing.T) {
			err := (&S3StorageConfig{SSE: mode}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown sse")
		})
	}
}

func TestS3StorageConfig_Validate_kms_key_arn(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/test-key"

	for _, mode := range []string{"aws:kms", "aws:kms:dsse"} {
		t.Run("allowed with "+mode, func(t *testing.T) {
			require.NoError(t, (&S3StorageConfig{SSE: mode, KMSKeyARN: arn}).Validate())
		})
	}

	// Silently dropping the key would encrypt the dump with a key the user did not ask for.
	for _, mode := range []string{"", "AES256"} {
		t.Run("rejected with sse "+mode, func(t *testing.T) {
			err := (&S3StorageConfig{SSE: mode, KMSKeyARN: arn}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "kms_key_arn requires sse")
		})
	}
}

func TestS3StorageConfig_Validate_bucket_key_enabled(t *testing.T) {
	for _, mode := range []string{"aws:kms", "aws:kms:dsse"} {
		t.Run("allowed with "+mode, func(t *testing.T) {
			require.NoError(t, (&S3StorageConfig{SSE: mode, BucketKeyEnabled: true}).Validate())
		})
	}

	for _, mode := range []string{"", "AES256"} {
		t.Run("rejected with sse "+mode, func(t *testing.T) {
			err := (&S3StorageConfig{SSE: mode, BucketKeyEnabled: true}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "bucket_key_enabled requires sse")
		})
	}
}

func TestNewS3StorageConfig_defaults(t *testing.T) {
	cfg := NewS3StorageConfig()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, "STANDARD", cfg.StorageClass)
	assert.True(t, cfg.ForcePathStyle)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.EqualValues(t, 50*1024*1024, cfg.MaxPartSize)
}

func TestAzureStorageConfig_Validate(t *testing.T) {
	t.Run("missing container", func(t *testing.T) {
		cfg := NewAzureStorageConfig()
		cfg.StorageAccount = "test-account"
		assert.EqualError(t, cfg.Validate(), "container is required")
	})

	t.Run("missing storage account", func(t *testing.T) {
		cfg := NewAzureStorageConfig()
		cfg.Container = "test-container"
		assert.EqualError(t, cfg.Validate(), "storage_account is required")
	})

	t.Run("clamps below-minimum values", func(t *testing.T) {
		cfg := NewAzureStorageConfig()
		cfg.Container = "test-container"
		cfg.StorageAccount = "test-account"
		cfg.BufferSize = 1
		cfg.MaxBuffers = 0
		cfg.TryTimeout = 0
		cfg.EnvironmentName = ""
		require.NoError(t, cfg.Validate())
		assert.Equal(t, minAzureBufferSize, cfg.BufferSize)
		assert.Equal(t, minAzureBuffers, cfg.MaxBuffers)
		assert.Equal(t, defaultAzureTryTimeout, cfg.TryTimeout)
		assert.Equal(t, defaultAzureEnvName, cfg.EnvironmentName)
	})
}

func TestNewAzureStorageConfig_defaults(t *testing.T) {
	cfg := NewAzureStorageConfig()
	assert.Equal(t, defaultAzureBufferSize, cfg.BufferSize)
	assert.Equal(t, defaultAzureBuffers, cfg.MaxBuffers)
	assert.Equal(t, defaultAzureTryTimeout, cfg.TryTimeout)
	assert.Equal(t, defaultAzureMaxRetries, cfg.MaxRetries)
	assert.Equal(t, defaultAzureEnvName, cfg.EnvironmentName)
}

func TestSSHStorageConfig_Validate(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *SSHStorageConfig
		wantErr  string
		wantPort int
	}{
		{name: "missing host", cfg: &SSHStorageConfig{User: "u", Password: "p", Port: 22}, wantErr: "host is required"},
		{name: "missing user", cfg: &SSHStorageConfig{Host: "h", Password: "p", Port: 22}, wantErr: "user is required"},
		{name: "missing auth", cfg: &SSHStorageConfig{Host: "h", User: "u", Port: 22}, wantErr: "one of password or private_key_path is required"},
		{
			name:    "passphrase without key",
			cfg:     &SSHStorageConfig{Host: "h", User: "u", Password: "p", PrivateKeyPassphrase: "x"},
			wantErr: "private_key_passphrase requires private_key_path",
		},
		{
			name:    "negative connect timeout",
			cfg:     &SSHStorageConfig{Host: "h", User: "u", Password: "p", ConnectTimeout: -time.Second},
			wantErr: "connect_timeout must not be negative",
		},
		{name: "valid with password", cfg: &SSHStorageConfig{Host: "h", User: "u", Password: "p", Port: 22}, wantPort: 22},
		{name: "valid with private key path", cfg: &SSHStorageConfig{Host: "h", User: "u", PrivateKeyPath: "/key", Port: 2222}, wantPort: 2222},
		{name: "non-positive port clamps to default", cfg: &SSHStorageConfig{Host: "h", User: "u", Password: "p"}, wantPort: defaultSSHPort},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPort, tt.cfg.Port)
		})
	}
}

func TestDirectoryStorageConfig_Validate(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))

	assert.ErrorIs(t, (&DirectoryStorageConfig{}).Validate(), ErrPathIsRequired)
	assert.ErrorIs(t, (&DirectoryStorageConfig{Path: filepath.Join(dir, "missing")}).Validate(), os.ErrNotExist)
	assert.Error(t, (&DirectoryStorageConfig{Path: file}).Validate())
	assert.NoError(t, (&DirectoryStorageConfig{Path: dir}).Validate())
}
