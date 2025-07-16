package config

import (
	"sync"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
)

var (
	cfg  *Config
	once sync.Once
)

// options is an interface for all dump options. Since we delegate schema dumping to the external tool,
// utilities, depending on the DBMS the set of parameters can be different.
type options interface {
	ConnectionConfig() (interfaces.ConnectionConfigurator, error)
	SchemaDumpParams() ([]string, error)
	Get(key string) (any, error)
	GetIncludedTables() []string
	GetExcludedTables() []string
	GetExcludedSchemas() []string
	GetIncludedSchemas() []string
	Env() ([]string, error)
}

func NewConfig() *Config {
	once.Do(
		func() {
			cfg = &Config{
				Common:  NewCommon(),
				Storage: NewStorageConfig(),
				// TODO: Consider how to forward two dependencies
				//	dump and restore cfg interfaces.
				Dump:    NewDump(),
				Restore: NewRestore(),
			}
		},
	)
	return cfg
}

type Config struct {
	Engine   string        `mapstructure:"engine" yaml:"engine" json:"engine"`
	Common   Common        `mapstructure:"common" yaml:"common" json:"common"`
	Log      Log           `mapstructure:"log" yaml:"log" json:"log"`
	Storage  StorageConfig `mapstructure:"storage" yaml:"storage" json:"storage"`
	Dump     Dump          `mapstructure:"dump" yaml:"dump" json:"dump"`
	Validate Validate      `mapstructure:"validate" yaml:"validate" json:"validate"`
	Restore  Restore       `mapstructure:"restore" yaml:"restore" json:"restore"`
}
