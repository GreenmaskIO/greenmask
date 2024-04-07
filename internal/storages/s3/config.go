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
}

func NewConfig() *Config {
	return &Config{
		StorageClass:   defaultStorageClass,
		ForcePathStyle: defaultForcePath,
		MaxRetries:     defaultMaxRetries,
		MaxPartSize:    defaultMaxPartSize,
	}
}
