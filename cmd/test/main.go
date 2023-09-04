package main

import (
	"os"

	"github.com/rs/zerolog/log"

	toclib "github.com/GreenmaskIO/greenmask/internal/db/postgres/toc"
)

func main() {
	src, err := os.Open("/home/vadim/tmp/pg_dump_test/1692197069492/toc.dat")
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer src.Close()
	dest, err := os.Create("/home/vadim/tmp/pg_dump_test/1692197069492/new_toc.dat")
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer dest.Close()

	reader := toclib.NewReader(src)
	toc, err := reader.Read()
	if err != nil {
		log.Fatal().Err(err).Msgf("err")
	}

	for _, item := range toc.Entries {
		if item.Section == toclib.SectionData {
			log.Printf("%+v\n", item)
		}
	}

	writer := toclib.NewWriter(dest)
	if err := writer.Write(toc); err != nil {
		log.Fatal().Err(err).Msgf("err")
	}
}
