package config

import (
	"sync"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/storages/s3"
)

var (
	Cfg  *Config
	once sync.Once
)

func NewConfig() *Config {
	once.Do(func() {
		Cfg = &Config{}
	})
	return Cfg
}

type Config struct {
	CommonV2 CommonV2      `mapstructure:"commonV2" yaml:"commonV2"`
	Log      LogConfig     `mapstructure:"log" yaml:"log"`
	Storage  StorageConfig `mapstructure:"storage" yaml:"storage"`
	Dump     Dump          `mapstructure:"dump" yaml:"dump"`
	Restore  Restore       `mapstructure:"restore" yaml:"restore"`

	// Common
	// Deprecated
	Common Common `mapstructure:"common" yaml:"common"`
}

type CommonV2 struct {
	PgBinPath string `mapstructure:"pgBinPath" yaml:"pgBinPath,omitempty"`
}

type StorageConfig struct {
	S3        s3.Config        `mapstructure:"s3"`
	Directory directory.Config `mapstructure:"directory"`
}

type LogConfig struct {
	Format string `mapstructure:"format" yaml:"format"`
	Level  string `mapstructure:"level" yaml:"level"`
}

// Deprecated
type Common struct {
	LogFormat string  `mapstructure:"log-format" yaml:"logFormat"`
	LogLevel  string  `mapstructure:"log-level" yaml:"logLevel"`
	BinPath   string  `mapstructure:"bin_path" yaml:"bin_path,omitempty"`
	Storage   Storage `mapstructure:"storage" yaml:"storage"`
}

// Deprecated
type Storage struct {
	Type      string    `mapstructure:"type" yaml:"type"`
	Directory Directory `mapstructure:"directory" yaml:"directory"`
}

// Deprecated
type Directory struct {
	Path string `mapstructure:"path" yaml:"path"`
}

type Dump struct {
	PgDumpOptions  pgdump.Options `mapstructure:"pg_dump_options" yaml:"pgDumpOptions"`
	Transformation []*Table       `mapstructure:"transformation" yaml:"transformation"`
}

type Restore struct {
	PgRestoreOptions pgrestore.Options             `mapstructure:"pg_restore_options" yaml:"pgRestoreOptions"`
	Scripts          map[string][]pgrestore.Script `mapstructure:"scripts" yaml:"scripts"`
}

type TransformerConfig struct {
	Name   string            `mapstructure:"name" yaml:"name"`
	Params map[string][]byte `mapstructure:"params" yaml:"params"`
}

type Table struct {
	Schema       string               `mapstructure:"schema" yaml:"schema"`
	Name         string               `mapstructure:"name" yaml:"name"`
	Query        string               `mapstructure:"query" yaml:"query"`
	Transformers []*TransformerConfig `mapstructure:"transformers" yaml:"transformers"`
}

func init() {
	//mapstructure.Decoder{}
}
