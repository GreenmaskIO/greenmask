package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/mitchellh/mapstructure"

	"github.com/GreenmaskIO/greenmask/internal/db/postgres/pgdump"
	pgrestore2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/pgrestore"
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
	Common     Common  `mapstructure:"common" yaml:"common"`
	Dump       Dump    `mapstructure:"dump" yaml:"dump"`
	Restore    Restore `mapstructure:"restore" yaml:"restore"`
	configPath string
}

type Common struct {
	LogFormat string  `mapstructure:"log-format" yaml:"logFormat"`
	LogLevel  string  `mapstructure:"log-level" yaml:"logLevel"`
	BinPath   string  `mapstructure:"bin_path" yaml:"bin_path,omitempty"`
	Storage   Storage `mapstructure:"storage" yaml:"storage"`
}

type Storage struct {
	Type      string    `mapstructure:"type" yaml:"type"`
	Directory Directory `mapstructure:"directory" yaml:"directory"`
}

type Directory struct {
	Path string `mapstructure:"path" yaml:"path"`
}

type Dump struct {
	PgDumpOptions  pgdump.Options `mapstructure:"pg_dump_options" yaml:"pgDumpOptions"`
	Transformation []*Table       `mapstructure:"transformation" yaml:"transformation"`
}

type Restore struct {
	PgRestoreOptions pgrestore2.Options             `mapstructure:"pg_restore_options" yaml:"pgRestoreOptions"`
	Scripts          map[string][]pgrestore2.Script `mapstructure:"scripts" yaml:"scripts"`
}

//type ParameterValue []byte

func ParamsToByteSliceHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t != reflect.TypeOf([]byte{}) {
			return data, nil
		}

		switch v := data.(type) {
		case string:
			return []byte(v), nil
		default:
			res, err := json.Marshal(data)
			if err != nil {
				return nil, fmt.Errorf("cannot convert map to yaml bytes: %w", err)
			}
			return res, nil
		}
	}
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
