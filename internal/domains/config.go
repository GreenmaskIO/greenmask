package domains

import (
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
)

type Config struct {
	BinPath       string          `mapstructure:"bin-path"`
	PgDumpOptions *pgdump.Options `mapstructure:",squash"`
	configPath    string
	YamlConfig    []Table `mapstructure:"transformers"`
}
