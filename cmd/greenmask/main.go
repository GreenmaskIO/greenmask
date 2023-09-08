package main

import (
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
