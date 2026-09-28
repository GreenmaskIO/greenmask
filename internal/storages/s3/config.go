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

package s3

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	defaultMaxRetries   = 3
	defaultMaxPartSize  = 50 * 1024 * 1024
	defaultStorageClass = "STANDARD"
	defaultForcePath    = true
)

type Config struct {
	Endpoint         string `mapstructure:"endpoint"`
	Bucket           string `mapstructure:"bucket"`
	Prefix           string `mapstructure:"prefix"`
	Region           string `mapstructure:"region"`
	StorageClass     string `mapstructure:"storage_class"`
	DisableSSL       bool   `mapstructure:"disable_ssl"`
	AccessKeyId      string `mapstructure:"access_key_id"`
	SecretAccessKey  string `mapstructure:"secret_access_key"`
	SessionToken     string `mapstructure:"session_token"`
	RoleArn          string `mapstructure:"role_arn"`
	SessionName      string `mapstructure:"session_name"`
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

func NewConfig() *Config {
	return &Config{
		StorageClass:   defaultStorageClass,
		ForcePathStyle: defaultForcePath,
		MaxRetries:     defaultMaxRetries,
		MaxPartSize:    defaultMaxPartSize,
	}
}

// sseIsKMS reports whether the configured SSE mode is one of the KMS-backed
// modes, which are the only ones accepting kms_key_arn and bucket_key_enabled.
func (c *Config) sseIsKMS() bool {
	return c.SSE == s3.ServerSideEncryptionAwsKms || c.SSE == s3.ServerSideEncryptionAwsKmsDsse
}

// Validate rejects an incoherent encryption setup at startup instead of
// letting it fail on the first upload, once the dump has already been made.
func (c *Config) Validate() error {
	switch c.SSE {
	case "", s3.ServerSideEncryptionAes256, s3.ServerSideEncryptionAwsKms, s3.ServerSideEncryptionAwsKmsDsse:
	default:
		return fmt.Errorf(
			"unknown sse %q: must be one of %s, %s, %s", c.SSE,
			s3.ServerSideEncryptionAes256, s3.ServerSideEncryptionAwsKms, s3.ServerSideEncryptionAwsKmsDsse,
		)
	}

	if c.sseIsKMS() {
		return nil
	}
	if c.KMSKeyARN != "" {
		return fmt.Errorf(
			"kms_key_arn requires sse to be %s or %s",
			s3.ServerSideEncryptionAwsKms, s3.ServerSideEncryptionAwsKmsDsse,
		)
	}
	if c.BucketKeyEnabled {
		return fmt.Errorf(
			"bucket_key_enabled requires sse to be %s or %s",
			s3.ServerSideEncryptionAwsKms, s3.ServerSideEncryptionAwsKmsDsse,
		)
	}
	return nil
}

// applyEncryption sets the server-side encryption headers on an upload.
// Validate guarantees the KMS-only options are set only with a KMS-backed mode.
func (c *Config) applyEncryption(ui *s3manager.UploadInput) {
	if c.SSE == "" {
		return
	}
	ui.ServerSideEncryption = aws.String(c.SSE)
	if c.KMSKeyARN != "" {
		ui.SSEKMSKeyId = aws.String(c.KMSKeyARN)
	}
	if c.BucketKeyEnabled {
		ui.BucketKeyEnabled = aws.Bool(true)
	}
}
