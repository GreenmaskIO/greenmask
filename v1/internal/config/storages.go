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

package config

import (
	"github.com/greenmaskio/greenmask/v1/internal/storages/directory"
	"github.com/greenmaskio/greenmask/v1/internal/storages/s3"
)

const (
	defaultStorageType = "directory"
)

type DirectoryConfig struct {
	Path string `mapstructure:"path"`
}

func NewStorageDirectory() DirectoryConfig {
	return DirectoryConfig{}
}

func (d DirectoryConfig) ToDirectoryConfig() directory.DirectoryConfig {
	return directory.NewDirectoryConfig(d.Path)
}

type S3Config struct {
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
	ForcePathStyle   *bool  `mapstructure:"force_path_style"`
	UseAccelerate    bool   `mapstructure:"use_accelerate"`
	NoVerifySsl      bool   `mapstructure:"no_verify_ssl"`
}

func NewStorageS3() S3Config {
	return S3Config{
		MaxRetries:  -1,
		MaxPartSize: -1,
	}
}

func (st S3Config) ToS3Config() s3.S3Config {
	return s3.NewS3Config(
		st.Endpoint,
		st.Bucket,
		st.Prefix,
		st.Region,
		st.StorageClass,
		st.AccessKeyId,
		st.SecretAccessKey,
		st.SessionToken,
		st.RoleArn,
		st.SessionName,
		st.MaxRetries,
		st.CertFile,
		st.MaxPartSize,
		st.Concurrency,
		st.UseListObjectsV1,
		st.ForcePathStyle,
		st.UseAccelerate,
		st.NoVerifySsl,
	)
}

type StorageConfig struct {
	Type      string          `mapstructure:"type" yaml:"type" json:"type,omitempty"`
	S3        S3Config        `mapstructure:"s3"  json:"s3,omitempty" yaml:"s3"`
	Directory DirectoryConfig `mapstructure:"directory" json:"directory,omitempty" yaml:"directory"`
}

func NewStorageConfig() StorageConfig {
	return StorageConfig{
		Type:      defaultStorageType,
		S3:        NewStorageS3(),
		Directory: NewStorageDirectory(),
	}
}
