package config

import "github.com/greenmaskio/greenmask/v1/internal/storages"

const (
	defaultStorageType = "directory"
)

type DirectoryConfig struct {
	Path string `mapstructure:"path"`
}

func NewStorageDirectory() DirectoryConfig {
	return DirectoryConfig{}
}

func (d DirectoryConfig) ToDirectoryConfig() storages.DirectoryConfig {
	return storages.NewDirectoryConfig(d.Path)
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

func (s3 S3Config) ToS3Config() storages.S3Config {
	return storages.NewS3Config(
		s3.Endpoint,
		s3.Bucket,
		s3.Prefix,
		s3.Region,
		s3.StorageClass,
		s3.AccessKeyId,
		s3.SecretAccessKey,
		s3.SessionToken,
		s3.RoleArn,
		s3.SessionName,
		s3.MaxRetries,
		s3.CertFile,
		s3.MaxPartSize,
		s3.Concurrency,
		s3.UseListObjectsV1,
		s3.ForcePathStyle,
		s3.UseAccelerate,
		s3.NoVerifySsl,
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
