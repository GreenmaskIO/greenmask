package domains

import (
	"flag"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	"sync"
)

var (
	once   = sync.Once{}
	config *Config
)

type Config struct {
	BinPath       string
	PgDumpOptions *pgdump.Options
}

func NewConfig() *Config {
	once.Do(func() {
		binPath := flag.String("binpath", "/usr/bin", "")
		dbname := flag.String("dbname", "postgres", "")
		host := flag.String("host", "localhost", "")
		port := flag.Int("port", 5432, "")
		userName := flag.String("username", "postgres", "")
		fileName := flag.String("file", "", "")

		flag.Parse()

		config = &Config{
			BinPath: *binPath,
			PgDumpOptions: &pgdump.Options{
				FileName: *fileName,
				DbName:   *dbname,
				Host:     *host,
				Port:     *port,
				UserName: *userName,
			},
		}
	})
	return config
}
