package config

import (
	"sync"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	pgrestore2 "github.com/wwoytenko/greenfuscator/internal/db/postgres/pgrestore"
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
	Common     Common  `mapstructure:"common"`
	Dump       Dump    `mapstructure:"dump"`
	Restore    Restore `mapstructure:"restore"`
	configPath string
}

type Common struct {
	LogFormat string  `mapstructure:"log-format"`
	LogLevel  string  `mapstructure:"log-level"`
	BinPath   string  `mapstructure:"bin_path" json:"bin_path,omitempty"`
	Storage   Storage `mapstructure:"storage" json:"storage"`
}

type Storage struct {
	Type      string    `mapstructure:"type"`
	Directory Directory `mapstructure:"directory"`
}

type Directory struct {
	Path string `mapstructure:"path"`
}

type Dump struct {
	PgDumpOptions  pgdump.Options `mapstructure:"pg_dump_options"`
	Transformation []*Table       `mapstructure:"transformation"`
}

type Restore struct {
	PgRestoreOptions pgrestore2.Options             `mapstructure:"pg_restore_options"`
	Scripts          map[string][]pgrestore2.Script `mapstructure:"scripts"`
}

type TransformerConfig struct {
	Name   string            `mapstructure:"name"`
	Params map[string][]byte `mapstructure:"params"`
}

type Table struct {
	Schema       string               `mapstructure:"schema"`
	Name         string               `mapstructure:"name"`
	Query        string               `mapstructure:"query"`
	Transformers []*TransformerConfig `mapstructure:"transformers"`
}
