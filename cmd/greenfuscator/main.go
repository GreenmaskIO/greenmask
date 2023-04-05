package main

import (
	"os"
	"time"

	"github.com/wwoytenko/greenfuscator/internal/postgres/lib/toc"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// We can use:
// pg_dump -U postgres -d test -Fd --schema-only -f ./
// For determining the order you can use the TABLE definition order

func main() {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	log.Logger = logger

	srcFile, err := os.Open("/tmp/pg_dump_test/diff_test/original/toc.dat")
	if err != nil {
		log.Fatal().Err(err)
	}
	defer srcFile.Close()

	destFile, err := os.Create("/tmp/pg_dump_test/test2/toc.dat")
	if err != nil {
		log.Fatal().Err(err)
	}
	defer destFile.Close()

	ah := toc.NewArchiveHandle(srcFile, destFile, toc.ArchDirectory)
	if err := ReadExistedTocFile(ah); err != nil {
		log.Fatal().Err(err).Msg("error")
	}

	if err := WriteTocData(ah); err != nil {
		log.Fatal().Err(err).Msg("error")
	}

}

func ChangeEntities() {
	// TODO:
	//	1. Find the table definition
}

func ReadExistedTocFile(ah *toc.ArchiveHandle) error {
	if err := ah.ReadHead(); err != nil {
		return err
	}

	if err := ah.ReadToc(); err != nil {
		return err
	}
	return nil
}

func WriteTocData(ah *toc.ArchiveHandle) error {
	if err := ah.WriteHead(); err != nil {
		return err
	}

	if err := ah.WriteToc(); err != nil {
		return err
	}
	return nil
}
