package main

import (
	"os"
	"path"

	"github.com/rs/zerolog/log"

	toclib "github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func main() {
	dirPath := os.Args[1]
	src, err := os.Open(path.Join(dirPath, "pg_dump", "toc.dat"))
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer src.Close()
	dest, err := os.Create(path.Join(dirPath, "pg_dump", "new_toc2.dat"))
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
