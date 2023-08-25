package main

import (
	"os"

	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
)

func main() {
	f, err := os.Open("/home/vadim/tmp/pg_dump_test/vanil/toc.dat")
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	res, err := os.Create("/home/vadim/tmp/pg_dump_test/vanil/new_toc.dat")
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer f.Close()
	defer res.Close()

	ah, err := toc.ReadFile(f)
	if err != nil {
		log.Fatal().Err(err).Msgf("err")
	}

	for _, item := range ah.GetEntries() {
		if item.Section == toc.SectionData {
			log.Printf("%+v\n", item)
		}
	}

	if err := toc.WriteFile(ah, res); err != nil {
		log.Fatal().Err(err).Msgf("err")
	}
}
