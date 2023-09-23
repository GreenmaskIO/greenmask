package main

import (
	"os"

	"github.com/rs/zerolog/log"

	toclib "github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func main() {
	src, err := os.Open("/Users/vadim/tmp/pg_dump_test/backward_compatibility_test_1112907377/storage/1695457451229/test1/toc.dat")
	//src, err := os.Open("/Users/vadim/tmp/pg_dump_test/backward_compatibility_test_1112907377/storage/1695457451229/toc.dat")
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer src.Close()
	dest, err := os.Create("/Users/vadim/tmp/pg_dump_test/backward_compatibility_test_1112907377/storage/1695457451229/test1/new_toc.dat")
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
