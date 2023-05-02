package domains

import (
	"sync"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
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
	Common     Common `mapstructure:"common"`
	Dump       Dump   `mapstructure:"dump"`
	configPath string
}

type Common struct {
	BinPath string  `mapstructure:"bin_path"`
	Storage Storage `mapstructure:"storage"`
}

type Storage struct {
	Type      string    `mapstructure:"type"`
	Directory Directory `mapstructure:"directory"`
}

type Directory struct {
	Path string `mapstructure:"path"`
}

type Dump struct {
	PgDumpOptions pgdump.Options `mapstructure:"pg_dump_options"`
	Transformers  []Table        `mapstructure:"transformers"`
}
