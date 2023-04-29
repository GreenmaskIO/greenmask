package domains

import (
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
)

type Config struct {
	BinPath       string          `mapstructure:"bin_path"`
	PgDumpOptions *pgdump.Options `mapstructure:"pg_dump_options"`
	configPath    string
	YamlConfig    []Table `mapstructure:"transformers"`
}
