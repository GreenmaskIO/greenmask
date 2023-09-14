package s3

import (
	"errors"
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
	StorageClass     string `mapstructure:"storageClass,omitempty"`
	DisableSSL       bool   `mapstructure:"disableSsl,omitempty"`
	AccessKeyId      string `mapstructure:"accessKeyId,omitempty"`
	SecretAccessKey  string `mapstructure:"secretAccessKey,omitempty"`
	SessionToken     string `mapstructure:"sessionToken,omitempty"`
	RoleArn          string `mapstructure:"roleArn,omitempty"`
	SessionName      string `mapstructure:"sessionName,omitempty"`
	MaxRetries       int    `mapstructure:"maxRetries,omitempty"`
	CertFile         string `mapstructure:"certFile,omitempty"`
	MaxPartSize      int64  `mapstructure:"maxPartSize,omitempty"`
	Concurrency      int    `mapstructure:"concurrency,omitempty"`
	UseListObjectsV1 bool   `mapstructure:"useListObjectsV1,omitempty"`
	ForcePathStyle   bool   `mapstructure:"forcePathStyle,omitempty"`
	UseAccelerate    bool   `mapstructure:"useAccelerate,omitempty"`
}

func NewConfig() *Config {
	return &Config{
		StorageClass:   "STANDARD",
		ForcePathStyle: true,
		MaxRetries:     defaultMaxRetries,
		//MaxPartSize:    defaultMaxPartSize,
	}
}

func (c *Config) Validate() error {
	if c.Region != "" {
		return errors.New("region cannot be empty")
	}
	return nil
}
