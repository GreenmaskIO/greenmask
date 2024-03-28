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
	"errors"
	"os"
)

const (
	defaultMaxRetries  = 3
	defaultMaxPartSize = 50 * 1024 * 1024
)

type Config struct {
	Endpoint         string `mapstructure:"endpoint,omitempty"`
	Bucket           string `mapstructure:"bucket,omitempty"`
	Prefix           string `mapstructure:"prefix,omitempty"`
	Region           string `mapstructure:"region,omitempty"`
	StorageClass     string `mapstructure:"storage_class,omitempty"`
	DisableSSL       bool   `mapstructure:"disable_ssl,omitempty"`
	AccessKeyId      string `mapstructure:"access_key_id,omitempty"`
	SecretAccessKey  string `mapstructure:"secret_access_key,omitempty"`
	SessionToken     string `mapstructure:"session_token,omitempty"`
	RoleArn          string `mapstructure:"role_arn,omitempty"`
	SessionName      string `mapstructure:"session_name,omitempty"`
	MaxRetries       int    `mapstructure:"max_retries,omitempty"`
	CertFile         string `mapstructure:"cert_file,omitempty"`
	MaxPartSize      int64  `mapstructure:"max_part_size,omitempty"`
	Concurrency      int    `mapstructure:"concurrency,omitempty"`
	UseListObjectsV1 bool   `mapstructure:"use_list_objects_v1,omitempty"`
	ForcePathStyle   bool   `mapstructure:"force_path_style,omitempty"`
	UseAccelerate    bool   `mapstructure:"use_accelerate,omitempty"`
}

func NewConfig() *Config {
	return &Config{
		StorageClass:   "STANDARD",
		ForcePathStyle: true,
		MaxRetries:     defaultMaxRetries,
		MaxPartSize:    defaultMaxPartSize,
		Bucket:   		os.Getenv("STORAGE_S3_BUCKET_NAME"),
		Region: 		os.Getenv("STORAGE_S3_BUCKET_REGION"),
		Prefix:			os.Getenv("STORAGE_S3_BUCKET_PREFIX"),
	}
}

func (c *Config) Validate() error {
	if c.Region != "" {
		return errors.New("region cannot be empty")
	}
	return nil
}
