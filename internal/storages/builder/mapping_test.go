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

package builder

import (
	"strings"
	"testing"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/greenmaskio/storages/azure"
	"github.com/greenmaskio/storages/directory"
	"github.com/greenmaskio/storages/s3"
	"github.com/greenmaskio/storages/ssh"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/domains"
)

func newStorageConfig() *domains.StorageConfig {
	return &domains.StorageConfig{
		S3:        domains.NewS3StorageConfig(),
		Azure:     domains.NewAzureStorageConfig(),
		Directory: domains.NewDirectoryStorageConfig(),
		SSH:       domains.NewSSHStorageConfig(),
	}
}

// decodeStorage decodes the storage section of a YAML config the way cmd/greenmask/cmd/root.go does.
func decodeStorage(t *testing.T, v *viper.Viper) *domains.StorageConfig {
	t.Helper()
	cfg := struct {
		Storage *domains.StorageConfig `mapstructure:"storage"`
	}{Storage: newStorageConfig()}
	err := v.Unmarshal(&cfg, func(dc *mapstructure.DecoderConfig) {
		dc.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		)
		dc.ErrorUnused = true
	})
	require.NoError(t, err)
	return cfg.Storage
}

func decodeYAML(t *testing.T, doc string) *domains.StorageConfig {
	t.Helper()
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(doc)))
	return decodeStorage(t, v)
}

// s3Defaults is what greenmask's own defaults map to, before any key is set.
func s3Defaults() s3.Config {
	return s3.Config{
		StorageClass:   "STANDARD",
		MaxRetries:     4,
		MaxPartSize:    50 * 1024 * 1024,
		ForcePathStyle: true,
	}
}

func TestToS3(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want func(c *s3.Config)
	}{
		{
			name: "defaults",
			yaml: "storage: {type: s3}",
			want: func(*s3.Config) {},
		},
		{
			name: "minio example from docs",
			yaml: `
storage:
  type: "s3"
  s3:
    endpoint: "http://localhost:9000"
    bucket: "testbucket"
    region: "us-east-1"
    access_key_id: "Q3AM3UQ867SPQQA43P2F"
    secret_access_key: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
`,
			want: func(c *s3.Config) {
				c.Endpoint = "http://localhost:9000"
				c.Bucket = "testbucket"
				c.Region = "us-east-1"
				c.AccessKeyId = "Q3AM3UQ867SPQQA43P2F"
				c.SecretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
			},
		},
		{
			name: "r2 example from docs",
			yaml: `
storage:
  type: "s3"
  s3:
    endpoint: "https://acc.r2.cloudflarestorage.com"
    bucket: "my-bucket"
    prefix: "backups"
    region: "us-east-1"
    force_path_style: true
    access_key_id: "ak"
    secret_access_key: "sk"
`,
			want: func(c *s3.Config) {
				c.Endpoint = "https://acc.r2.cloudflarestorage.com"
				c.Bucket = "my-bucket"
				c.Prefix = "backups"
				c.Region = "us-east-1"
				c.AccessKeyId = "ak"
				c.SecretAccessKey = "sk"
			},
		},
		{
			name: "endpoint without scheme gets https",
			yaml: "storage: {s3: {endpoint: s3.example.com}}",
			want: func(c *s3.Config) { c.Endpoint = "https://s3.example.com" },
		},
		{
			name: "endpoint with port and no scheme",
			yaml: "storage: {s3: {endpoint: 'minio:9000'}}",
			want: func(c *s3.Config) { c.Endpoint = "https://minio:9000" },
		},
		{
			name: "endpoint with path and no scheme",
			yaml: "storage: {s3: {endpoint: 'ceph.local/rgw'}}",
			want: func(c *s3.Config) { c.Endpoint = "https://ceph.local/rgw" },
		},
		{
			name: "http endpoint is kept",
			yaml: "storage: {s3: {endpoint: 'http://minio:9000'}}",
			want: func(c *s3.Config) { c.Endpoint = "http://minio:9000" },
		},
		{
			// Same as v1's AddScheme: a colon before :// means it is not a scheme.
			name: "scheme-like text after host and port is not a scheme",
			yaml: "storage: {s3: {endpoint: 'minio:9000/x?u=http://y'}}",
			want: func(c *s3.Config) { c.Endpoint = "https://minio:9000/x?u=http://y" },
		},
		{
			name: "https endpoint is kept",
			yaml: "storage: {s3: {endpoint: 'https://s3.example.com:8443/path'}}",
			want: func(c *s3.Config) { c.Endpoint = "https://s3.example.com:8443/path" },
		},
		{
			name: "max_retries 0 is a single attempt",
			yaml: "storage: {s3: {max_retries: 0}}",
			want: func(c *s3.Config) { c.MaxRetries = 1 },
		},
		{
			name: "max_retries 3 is four attempts",
			yaml: "storage: {s3: {max_retries: 3}}",
			want: func(c *s3.Config) { c.MaxRetries = 4 },
		},
		{
			name: "negative max_retries is a single attempt",
			yaml: "storage: {s3: {max_retries: -1}}",
			want: func(c *s3.Config) { c.MaxRetries = 1 },
		},
		{
			name: "force_path_style false is passed through",
			yaml: "storage: {s3: {force_path_style: false}}",
			want: func(c *s3.Config) { c.ForcePathStyle = false },
		},
		{
			name: "sse with kms",
			yaml: "storage: {s3: {sse: 'aws:kms', kms_key_arn: 'arn:aws:kms:k', bucket_key_enabled: true}}",
			want: func(c *s3.Config) {
				c.SSE = "aws:kms"
				c.KMSKeyARN = "arn:aws:kms:k"
				c.BucketKeyEnabled = true
			},
		},
		{
			name: "remaining keys map 1:1",
			yaml: `
storage:
  s3:
    storage_class: GLACIER
    disable_ssl: true
    session_token: st
    role_arn: arn:aws:iam::1:role/r
    session_name: sn
    cert_file: /ca.pem
    max_part_size: 1024
    concurrency: 7
    use_list_objects_v1: true
    use_accelerate: true
    no_verify_ssl: true
`,
			want: func(c *s3.Config) {
				c.StorageClass = "GLACIER"
				c.DisableSSL = true
				c.SessionToken = "st"
				c.RoleArn = "arn:aws:iam::1:role/r"
				c.SessionName = "sn"
				c.CertFile = "/ca.pem"
				c.MaxPartSize = 1024
				c.Concurrency = 7
				c.UseListObjectsV1 = true
				c.UseAccelerate = true
				c.NoVerifySsl = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := s3Defaults()
			tt.want(&want)
			assert.Equal(t, want, toS3(decodeYAML(t, tt.yaml).S3))
		})
	}
}

func TestToAzure(t *testing.T) {
	got := toAzure(decodeYAML(t, `
storage:
  type: "azure"
  azure:
    storage_account: "mystorageaccount"
    container: "mycontainer"
    prefix: "backups"
    access_key: "key"
    sas_token: "sas"
    endpoint: "http://127.0.0.1:10000/devstoreaccount1"
    endpoint_suffix: "core.example"
    environment_name: "AzureChinaCloud"
    buffer_size: 2048
    max_buffers: 2
    try_timeout: 7
    max_retries: -1
    blob_store_api_version: "2021-08-06"
`).Azure)
	assert.Equal(t, azure.Config{
		Container:           "mycontainer",
		StorageAccount:      "mystorageaccount",
		Prefix:              "backups",
		AccessKey:           "key",
		SASToken:            "sas",
		Endpoint:            "http://127.0.0.1:10000/devstoreaccount1",
		EndpointSuffix:      "core.example",
		EnvironmentName:     "AzureChinaCloud",
		BufferSize:          2048,
		MaxBuffers:          2,
		TryTimeout:          7,
		MaxRetries:          -1,
		BlobStoreAPIVersion: "2021-08-06",
	}, got)

	defaults := toAzure(decodeYAML(t, "storage: {azure: {container: c, storage_account: a}}").Azure)
	assert.Equal(t, azure.Config{
		Container:       "c",
		StorageAccount:  "a",
		EnvironmentName: "AzurePublicCloud",
		BufferSize:      8 * 1024 * 1024,
		MaxBuffers:      4,
		TryTimeout:      5,
		MaxRetries:      3,
	}, defaults)
}

func TestToAzure_max_retries(t *testing.T) {
	tests := []struct {
		yaml string
		want int
	}{
		{yaml: "storage: {azure: {}}", want: 3},
		{yaml: "storage: {azure: {max_retries: 5}}", want: 5},
		// azcore reads 0 as its default of 3, so "no retries" must reach it as -1.
		{yaml: "storage: {azure: {max_retries: 0}}", want: -1},
		{yaml: "storage: {azure: {max_retries: -2}}", want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.yaml, func(t *testing.T) {
			assert.Equal(t, tt.want, toAzure(decodeYAML(t, tt.yaml).Azure).MaxRetries)
		})
	}
}

func TestToSSH(t *testing.T) {
	got := toSSH(decodeYAML(t, `
storage:
  type: ssh
  ssh:
    host: sftp.example.com
    port: 2222
    user: greenmask
    password: pw
    private_key_path: /home/greenmask/.ssh/id_ed25519
    private_key_passphrase: secret
    prefix: /backups/greenmask
    connect_timeout: 45s
`).SSH)
	assert.Equal(t, ssh.Config{
		Host:                 "sftp.example.com",
		Port:                 2222,
		User:                 "greenmask",
		Password:             "pw",
		PrivateKeyPath:       "/home/greenmask/.ssh/id_ed25519",
		PrivateKeyPassphrase: "secret",
		Prefix:               "/backups/greenmask",
		ConnectTimeout:       45 * time.Second,
	}, got)

	defaults := toSSH(decodeYAML(t, "storage: {ssh: {host: h, user: u}}").SSH)
	assert.Equal(t, ssh.Config{Host: "h", User: "u", Port: 22}, defaults)
}

func TestToDirectory(t *testing.T) {
	got := toDirectory(decodeYAML(t, `
storage:
  type: "directory"
  directory:
    path: "/home/user_name/storage_dir"
`).Directory)
	assert.Equal(t, directory.Config{RootPath: "/home/user_name/storage_dir"}, got)
}
