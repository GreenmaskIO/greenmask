package main

import (
	"context"
	"github.com/wwoytenko/greenfuscator/internal/postgres/client"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// We can use:
// pg_dump -U postgres -d test -Fd --schema-only -f ./
// For determining the order you can use the TABLE definition order

func main() {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	log.Logger = logger

	cli := client.NewPostgresClient("/usr/bin")
	if err := cli.Connect(context.Background(), "host=localhost user=postgres database=test"); err != nil {
		log.Fatal().Err(err)
	}

	cli.RunBackup()

	if err := DumpTable(con, &metricsTable); err != nil {
		log.Fatal().Err(err)
	}

}
