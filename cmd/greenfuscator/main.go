package main

import (
	"context"
	"os"
	"time"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/domains"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// We can use:
// pg_dump -U postgres -d test -Fd --schema-only -f ./
// For determining the order you can use the TABLE definition order

func main() {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	log.Logger = logger

	config := domains.NewConfig()
	if err := optionsCheck(config.PgDumpOptions); err != nil {
		log.Fatal().Err(err).Msg("option error")
	}

	pgObfuscator := postgres.NewObfuscator(config.BinPath, config.PgDumpOptions)

	if err := pgObfuscator.RunBackup(context.Background(), config.YamlConfig); err != nil {
		log.Fatal().Err(err).Msg("cannot make a backup")
	}

}

func optionsCheck(options *pgdump.Options) error {
	return nil
}
