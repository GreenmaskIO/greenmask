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
	"errors"
	"fmt"
	"os"
	"time"
)

const (
	defaultS3MaxRetries   = 3
	defaultS3MaxPartSize  = 50 * 1024 * 1024
	defaultS3StorageClass = "STANDARD"
	defaultS3ForcePath    = true

	sseAES256  = "AES256"
	sseKMS     = "aws:kms"
	sseKMSDSSE = "aws:kms:dsse"

	minAzureBufferSize     = 1024
	defaultAzureBufferSize = 8 * 1024 * 1024
	minAzureBuffers        = 1
	defaultAzureBuffers    = 4
	defaultAzureTryTimeout = 5
	defaultAzureMaxRetries = 3
	defaultAzureEnvName    = "AzurePublicCloud"

	defaultSSHPort = 22
)

var ErrPathIsRequired = errors.New("path is required")

type S3StorageConfig struct {
	Endpoint     string `mapstructure:"endpoint"`
	Bucket       string `mapstructure:"bucket"`
	Prefix       string `mapstructure:"prefix"`
	Region       string `mapstructure:"region"`
	StorageClass string `mapstructure:"storage_class"`
	// DisableSSL has no effect and is kept so that existing configs still load.
	DisableSSL      bool   `mapstructure:"disable_ssl"`
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	SessionToken    string `mapstructure:"session_token"`
	RoleArn         string `mapstructure:"role_arn"`
	SessionName     string `mapstructure:"session_name"`
	// MaxRetries is the number of retries after the first attempt.
	MaxRetries       int    `mapstructure:"max_retries"`
	CertFile         string `mapstructure:"cert_file"`
	MaxPartSize      int64  `mapstructure:"max_part_size"`
	Concurrency      int    `mapstructure:"concurrency"`
	UseListObjectsV1 bool   `mapstructure:"use_list_objects_v1"`
	ForcePathStyle   bool   `mapstructure:"force_path_style"`
	UseAccelerate    bool   `mapstructure:"use_accelerate"`
	NoVerifySsl      bool   `mapstructure:"no_verify_ssl"`
	SSE              string `mapstructure:"sse"`
	KMSKeyARN        string `mapstructure:"kms_key_arn"`
	BucketKeyEnabled bool   `mapstructure:"bucket_key_enabled"`
}

func NewS3StorageConfig() *S3StorageConfig {
	return &S3StorageConfig{
		StorageClass:   defaultS3StorageClass,
		ForcePathStyle: defaultS3ForcePath,
		MaxRetries:     defaultS3MaxRetries,
		MaxPartSize:    defaultS3MaxPartSize,
	}
}

func (c *S3StorageConfig) sseIsKMS() bool {
	return c.SSE == sseKMS || c.SSE == sseKMSDSSE
}

func (c *S3StorageConfig) Validate() error {
	switch c.SSE {
	case "", sseAES256, sseKMS, sseKMSDSSE:
	default:
		return fmt.Errorf("unknown sse %q: must be one of %s, %s, %s", c.SSE, sseAES256, sseKMS, sseKMSDSSE)
	}

	if c.sseIsKMS() {
		return nil
	}
	if c.KMSKeyARN != "" {
		return fmt.Errorf("kms_key_arn requires sse to be %s or %s", sseKMS, sseKMSDSSE)
	}
	if c.BucketKeyEnabled {
		return fmt.Errorf("bucket_key_enabled requires sse to be %s or %s", sseKMS, sseKMSDSSE)
	}
	return nil
}

type AzureStorageConfig struct {
	Container       string `mapstructure:"container"`        // required
	StorageAccount  string `mapstructure:"storage_account"`  // required
	Prefix          string `mapstructure:"prefix"`           // path within container
	AccessKey       string `mapstructure:"access_key"`       // auth: shared key
	SASToken        string `mapstructure:"sas_token"`        // auth: SAS
	Endpoint        string `mapstructure:"endpoint"`         // full service URL override (path-style, e.g. Azurite/private deployments)
	EndpointSuffix  string `mapstructure:"endpoint_suffix"`  // overrides env-derived suffix
	EnvironmentName string `mapstructure:"environment_name"` // AzurePublicCloud (default), AzureUSGovernmentCloud, AzureChinaCloud, AzureGermanCloud
	BufferSize      int    `mapstructure:"buffer_size"`      // upload block size, default 8MiB, min 1KiB
	MaxBuffers      int    `mapstructure:"max_buffers"`      // upload concurrency, default 4, min 1
	TryTimeout      int    `mapstructure:"try_timeout"`      // minutes, default 5
	// MaxRetries is the number of retries after a failed try, default 3, 0 or negative for none.
	MaxRetries          int    `mapstructure:"max_retries"`
	BlobStoreAPIVersion string `mapstructure:"blob_store_api_version"` // optional x-ms-version override
}

func NewAzureStorageConfig() *AzureStorageConfig {
	return &AzureStorageConfig{
		EnvironmentName: defaultAzureEnvName,
		BufferSize:      defaultAzureBufferSize,
		MaxBuffers:      defaultAzureBuffers,
		TryTimeout:      defaultAzureTryTimeout,
		MaxRetries:      defaultAzureMaxRetries,
	}
}

func (c *AzureStorageConfig) Validate() error {
	if c.Container == "" {
		return fmt.Errorf("container is required")
	}
	if c.StorageAccount == "" {
		return fmt.Errorf("storage_account is required")
	}
	if c.BufferSize < minAzureBufferSize {
		c.BufferSize = minAzureBufferSize
	}
	if c.MaxBuffers < minAzureBuffers {
		c.MaxBuffers = minAzureBuffers
	}
	if c.TryTimeout <= 0 {
		c.TryTimeout = defaultAzureTryTimeout
	}
	if c.EnvironmentName == "" {
		c.EnvironmentName = defaultAzureEnvName
	}
	return nil
}

type SSHStorageConfig struct {
	Host                 string `mapstructure:"host"`                   // required
	Port                 int    `mapstructure:"port"`                   // default 22
	User                 string `mapstructure:"user"`                   // required
	Password             string `mapstructure:"password"`               // auth: password
	PrivateKeyPath       string `mapstructure:"private_key_path"`       // auth: private key
	PrivateKeyPassphrase string `mapstructure:"private_key_passphrase"` // decrypts an encrypted private key
	Prefix               string `mapstructure:"prefix"`                 // remote root path
	// ConnectTimeout bounds the TCP dial and the SSH handshake, default 30s.
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
}

func NewSSHStorageConfig() *SSHStorageConfig {
	return &SSHStorageConfig{Port: defaultSSHPort}
}

func (c *SSHStorageConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Password == "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("one of password or private_key_path is required")
	}
	if c.PrivateKeyPassphrase != "" && c.PrivateKeyPath == "" {
		return fmt.Errorf("private_key_passphrase requires private_key_path")
	}
	if c.ConnectTimeout < 0 {
		return fmt.Errorf("connect_timeout must not be negative")
	}
	if c.Port <= 0 {
		c.Port = defaultSSHPort
	}
	return nil
}

type DirectoryStorageConfig struct {
	Path string `mapstructure:"path"`
}

func NewDirectoryStorageConfig() *DirectoryStorageConfig {
	return &DirectoryStorageConfig{}
}

func (d *DirectoryStorageConfig) Validate() error {
	if d.Path == "" {
		return ErrPathIsRequired
	}
	info, err := os.Stat(d.Path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("received directory path is file")
	}
	return nil
}
