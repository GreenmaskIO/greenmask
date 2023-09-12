package s3

import (
	"errors"
)

const (
	defaultMaxRetries  = 3
	defaultMaxPartSize = 50 * 1024 * 1024
)

type Config struct {
	Endpoint         string `mapstructure:"endpoint"`
	Bucket           string `mapstructure:"bucket"`
	Prefix           string `mapstructure:"prefix"`
	Region           string `mapstructure:"region"`
	StorageClass     string `mapstructure:"storageClass"`
	DisableSSL       bool   `mapstructure:"disableSsl"`
	AccessKeyId      string `mapstructure:"accessKeyId"`
	SecretAccessKey  string `mapstructure:"secretAccessKey"`
	SessionToken     string `mapstructure:"sessionToken"`
	RoleArn          string `mapstructure:"roleArn"`
	SessionName      string `mapstructure:"sessionName"`
	MaxRetries       int    `mapstructure:"maxRetries"`
	ForcePathStyle   bool   `mapstructure:"forcePathStyle"`
	CertFile         string `mapstructure:"certFile"`
	MaxPartSize      int64  `mapstructure:"maxPartSize"`
	Concurrency      int    `mapstructure:"concurrency"`
	UseListObjectsV1 bool   `mapstructure:"useListObjectsV1"`
	ForcePathStype   bool   `mapstructure:"forcePathStype"`
	UseAccelerate    bool   `mapstructure:"useAccelerate"`
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
