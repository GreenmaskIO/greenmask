package config

import (
	"sync"
)

var (
	cfg  *Config
	once sync.Once
)

// options is an interface for all dump options. Since we delegate schema dumping to the external tool,
// utilities, depending on the DBMS the set of parameters can be different.
type options interface {
	GetConnURI() (string, error)
	GetParams() ([]string, error)
}

func NewConfig(o options) *Config {
	once.Do(
		func() {
			cfg = &Config{
				Common:  NewCommon(),
				Storage: NewStorage(),
				Dump:    NewDump(o),
				Restore: NewRestore(o),
			}
		},
	)
	return cfg
}

type Config struct {
	Common   Common   `mapstructure:"common" yaml:"common" json:"common"`
	Log      Log      `mapstructure:"log" yaml:"log" json:"log"`
	Storage  Storage  `mapstructure:"storage" yaml:"storage" json:"storage"`
	Dump     Dump     `mapstructure:"dump" yaml:"dump" json:"dump"`
	Validate Validate `mapstructure:"validate" yaml:"validate" json:"validate"`
	Restore  Restore  `mapstructure:"restore" yaml:"restore" json:"restore"`
}
