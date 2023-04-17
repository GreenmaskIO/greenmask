package domains

import (
	"flag"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
	"os"
	"sync"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
)

var (
	once   = sync.Once{}
	config *Config
)

type Config struct {
	BinPath       string
	PgDumpOptions *pgdump.Options
	configPath    string
	YamlConfig    []Table `yaml:"transformers"`
}

func NewConfig() *Config {
	once.Do(func() {
		binPath := flag.String("binpath", "/usr/bin", "")
		dbname := flag.String("dbname", "postgres", "")
		host := flag.String("host", "localhost", "")
		port := flag.Int("port", 5432, "")
		userName := flag.String("username", "postgres", "")
		fileName := flag.String("file", "", "")
		configPath := flag.String("config", "", "")

		flag.Parse()

		yamlConfig := make([]Table, 0)
		config = &Config{
			BinPath:    *binPath,
			configPath: *configPath,
			YamlConfig: yamlConfig,
			PgDumpOptions: &pgdump.Options{
				FileName: *fileName,
				DbName:   *dbname,
				Host:     *host,
				Port:     *port,
				UserName: *userName,
			},
		}

		if configPath != nil {

			f, err := os.Open(*configPath)
			if err != nil {
				log.Fatal().Msgf("unable to open config file: %s", err)
			}
			defer f.Close()

			if err := yaml.NewDecoder(f).Decode(config); err != nil {
				log.Fatal().Msgf("unable to open config file: %s", err)
			}
		}

	})
	return config
}
